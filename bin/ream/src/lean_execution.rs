//! Lean execution service: drives an UNMODIFIED execution client (e.g. reth) from the
//! real lean consensus, over the **standard Engine API**, using Ream's own
//! [`ExecutionEngine`] client.
//!
//! This is an alternative to embedding reth as a library + ExEx (Ream EPF project /
//! PR #1392). It demonstrates that the lean consensus layer can keep the post-Merge
//! CL→EL split: every time the real lean fork choice advances the head, this service
//! drives one execution block on the EL via `forkchoiceUpdated`/`getPayload`/`newPayload`,
//! and mirrors lean's 3SF finality onto the EL's `finalizedBlockHash`.
//!
//! Note: the lean block does not yet *carry* an execution payload (lean blocks are
//! attestation-only), so the EL chain produced here is a consensus-coupled "shadow"
//! execution chain: it is driven 1:1 by real lean slots and finalized by real lean
//! finality, but the binding of an EL payload *into* a lean block is left as spec work.

use std::{collections::BTreeMap, time::Duration};

use alloy_primitives::{Address, B256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use anyhow::{Result, anyhow, bail};
use ream_execution_engine::ExecutionEngine;
use ream_execution_rpc_types::{
    forkchoice_update::{ForkchoiceStateV1, PayloadAttributesV3},
    payload_status::PayloadStatus,
};
use ream_fork_choice_lean::store::LeanStoreReader;
use ream_storage::tables::{field::REDBField, table::REDBTable};
use ssz_types::VariableList;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Drives an execution client from real lean consensus via the standard Engine API.
pub struct LeanExecutionService {
    engine: ExecutionEngine,
    reader: LeanStoreReader,
    fee_recipient: Address,
    // PROTOTYPE: a poll loop (every `poll_interval`) notices new lean slots; production should be
    // event-driven — hook the slot tick / subscribe to head & finality changes — rather than poll.
    poll_interval: Duration,
}

impl LeanExecutionService {
    pub fn new(
        engine: ExecutionEngine,
        reader: LeanStoreReader,
        fee_recipient: Address,
    ) -> Self {
        Self {
            engine,
            reader,
            fee_recipient,
            poll_interval: Duration::from_millis(500),
        }
    }

    /// Read the current lean head slot and finalized slot from the shared fork-choice store.
    async fn read_lean_head_and_finalized(&self) -> Option<(u64, u64)> {
        let store = self.reader.read().await;
        let db = store.store.lock().await;
        let head_root = db.head_provider().get().ok()?;
        let head_state = db.state_provider().get(head_root).ok()??;
        let finalized = db.latest_finalized_provider().get().ok()?;
        Some((head_state.slot, finalized.slot))
    }

    /// Drive one execution block on top of `parent`, returning (block_hash, timestamp, tx_count).
    async fn produce_el_block(
        &self,
        parent: B256,
        parent_ts: u64,
        finalized: B256,
    ) -> Result<(B256, u64, usize)> {
        // PROTOTYPE: timestamp = parent+1 just to stay strictly-increasing (Engine API requires it);
        // production uses the real slot time, time_at_slot(slot).
        let ts = parent_ts + 1;
        let attributes = PayloadAttributesV3 {
            timestamp: ts,
            // PROTOTYPE placeholder; production supplies RANDAO / a PQ-VRF value (open spec question).
            prev_randao: B256::ZERO,
            suggested_fee_recipient: self.fee_recipient,
            withdrawals: VariableList::empty(), // lean has no withdrawals — empty is correct, not a stub
            // PROTOTYPE placeholder; production supplies the real lean/beacon block root (EIP-4788).
            parent_beacon_block_root: B256::ZERO,
        };

        // 1) forkchoiceUpdated + attributes -> begin building.
        let fcu = self
            .engine
            .engine_forkchoice_updated_v3(
                ForkchoiceStateV1 {
                    head_block_hash: parent,
                    // PROTOTYPE: use parent as "safe"; production tracks the justified checkpoint.
                    safe_block_hash: parent,
                    finalized_block_hash: finalized,
                },
                Some(attributes),
            )
            .await?;
        let payload_id = fcu
            .payload_id
            .ok_or_else(|| anyhow!("EL returned no payloadId (status {:?})", fcu.payload_status.status))?;

        // PROTOTYPE: fixed build delay so the EL can assemble the payload (geth builds empty-first).
        // Production times getPayload to the slot clock instead of sleeping a fixed amount.
        sleep(Duration::from_millis(900)).await;

        // 2) getPayload.
        let payload = self.engine.engine_get_payload_v4(payload_id).await?;
        let block_hash = payload.execution_payload.block_hash;
        let new_ts = payload.execution_payload.timestamp;
        let tx_count = payload.execution_payload.transactions.len();

        // 3) newPayload (feed it back) — must be VALID.
        let status = self
            .engine
            .engine_new_payload_v4(
                payload.execution_payload,
                vec![], // no blob txs in this prototype -> no expected versioned hashes
                B256::ZERO, // PROTOTYPE: parent_beacon_block_root placeholder (see attributes above)
                payload.execution_requests,
            )
            .await?;
        if status.status != PayloadStatus::Valid {
            bail!("newPayloadV4 not VALID: {:?}", status.status);
        }

        // 4) forkchoiceUpdated -> canonicalize the new head.
        self.engine
            .engine_forkchoice_updated_v3(
                ForkchoiceStateV1 {
                    head_block_hash: block_hash,
                    safe_block_hash: parent,
                    finalized_block_hash: finalized,
                },
                None,
            )
            .await?;

        Ok((block_hash, new_ts, tx_count))
    }

    /// Run forever: mirror real lean consensus head/finality onto the EL.
    pub async fn start(self) {
        info!("lean execution service starting: driving EL via the standard Engine API");

        // Wait for the EL and read its genesis block as the anchor.
        let (genesis_hash, genesis_ts) = loop {
            match self
                .engine
                .eth_get_block_by_number(BlockNumberOrTag::Number(0), false)
                .await
            {
                Ok(block) => break (block.header.hash, block.header.timestamp),
                Err(err) => {
                    warn!("execution client not reachable yet ({err}); retrying in 2s");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        };
        info!(genesis = %genesis_hash, genesis_ts, "EL genesis; anchoring forkchoice");

        if let Err(err) = self
            .engine
            .engine_forkchoice_updated_v3(
                ForkchoiceStateV1 {
                    head_block_hash: genesis_hash,
                    safe_block_hash: genesis_hash,
                    finalized_block_hash: genesis_hash,
                },
                None,
            )
            .await
        {
            error!("startup forkchoiceUpdated failed: {err}");
            return;
        }

        let mut el_parent = genesis_hash;
        let mut el_parent_ts = genesis_ts;
        let mut el_finalized = genesis_hash;
        let mut slot_to_el: BTreeMap<u64, B256> = BTreeMap::new();
        slot_to_el.insert(0, genesis_hash);
        let mut last_driven_slot = 0u64;

        loop {
            sleep(self.poll_interval).await;

            let Some((lean_head_slot, lean_finalized_slot)) =
                self.read_lean_head_and_finalized().await
            else {
                continue;
            };

            // For each new lean slot, drive one EL block.
            while last_driven_slot < lean_head_slot {
                let target_slot = last_driven_slot + 1;

                match self
                    .produce_el_block(el_parent, el_parent_ts, el_finalized)
                    .await
                {
                    Ok((hash, ts, tx_count)) => {
                        info!(
                            lean_slot = target_slot,
                            el_block = %hash,
                            txs = tx_count,
                            "real lean consensus drove an EL block via the Engine API"
                        );
                        el_parent = hash;
                        el_parent_ts = ts;
                        slot_to_el.insert(target_slot, hash);
                        last_driven_slot = target_slot;
                    }
                    Err(err) => {
                        error!("failed to drive EL block for lean slot {target_slot}: {err}");
                        break;
                    }
                }
            }

            // Mirror lean finality onto the EL: finalize the EL block of the greatest
            // produced lean slot <= lean's finalized slot.
            if let Some((&final_slot, &final_hash)) =
                slot_to_el.range(..=lean_finalized_slot).next_back()
            {
                if final_hash != el_finalized {
                    el_finalized = final_hash;
                    match self
                        .engine
                        .engine_forkchoice_updated_v3(
                            ForkchoiceStateV1 {
                                head_block_hash: el_parent,
                                safe_block_hash: el_parent,
                                finalized_block_hash: el_finalized,
                            },
                            None,
                        )
                        .await
                    {
                        Ok(_) => info!(
                            lean_finalized_slot = final_slot,
                            el_finalized = %final_hash,
                            "mirrored lean finality onto the EL"
                        ),
                        Err(err) => warn!("finality forkchoiceUpdated failed: {err}"),
                    }
                }
            }
        }
    }
}
