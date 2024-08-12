use std::{
    //borrow::Borrow,
    //sync::Arc,
    time::Duration,
};

use tokio::time::sleep;
use zksync_dal::{ConnectionPool, Core, CoreDal};
//use zksync_prover_interface::outputs::L1BatchProofForL1;
//use zksync_types::protocol_version::{L1VerifierConfig, ProtocolSemanticVersion};

use crate::{
    implementations::resources::{
        object_store::ObjectStoreResource,
        pools::{MasterPool, PoolResource},
    },
    FromContext, IntoContext, StopReceiver, Task, TaskId, WiringError, WiringLayer,
};

#[derive(Debug)]
pub struct MockStruct {
    //blob_store: Arc<dyn ObjectStore>,
    pool: ConnectionPool<Core>,
    // l1_verifier_config: L1VerifierConfig,
}

pub struct MockStructLayer();

impl MockStructLayer {
    pub fn new() -> Self {
        Self()
    }
}

impl Default for MockStructLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockStruct {
    pub fn new(
        //blob_store: Arc<dyn ObjectStore>,
        pool: ConnectionPool<Core>,
        //  l1_verifier_config: L1VerifierConfig,
    ) -> Self {
        Self {
            //  blob_store,
            pool,
            //  l1_verifier_config,
        }
    }
}

#[derive(Debug, FromContext)]
#[context(crate = crate)]
pub struct Input {
    pub object_store: ObjectStoreResource,
    pub master_pool: PoolResource<MasterPool>,
}

#[derive(Debug, IntoContext)]
#[context(crate = crate)]
pub struct Output {
    #[context(task)]
    pub mock: MockStruct,
}

#[async_trait::async_trait]
impl WiringLayer for MockStructLayer {
    type Input = Input;
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "proof_api_layer"
    }

    async fn wire(self, input: Self::Input) -> Result<Self::Output, WiringError> {
        println!("\n\nACTUALLL WIRING {:#?}\n\n", input);

        let master_pool = input.master_pool.get().await.unwrap();

        Ok(Output {
            mock: MockStruct::new(
                //input.object_store.0,
                master_pool,
            ),
        })
    }
}

#[async_trait::async_trait]
impl Task for MockStruct {
    fn id(&self) -> TaskId {
        "proof_api_layer".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        loop {
            let mut storage = self.pool.connection_tagged("proof_api").await.unwrap();

            println!("\nInside mock struct loop: ✅\n");
            // Example operation: sleeping for a short duration

            let previous_proven_batch_number = storage
                .blocks_dal()
                .get_last_l1_batch_with_prove_tx()
                .await
                .unwrap();
            let batch_to_prove = previous_proven_batch_number + 1;

            // let minor_version = storage
            //     .blocks_dal()
            //     .get_batch_protocol_version_id(batch_to_prove)
            //     .await
            //     .unwrap();

            // // `l1_verifier_config.recursion_scheduler_level_vk_hash` is a VK hash that L1 uses.
            // // We may have multiple versions with different verification keys, so we check only for proofs that use
            // // keys that correspond to one on L1.
            // let allowed_patch_versions = storage
            //     .protocol_versions_dal()
            //     .get_patch_versions_for_vk(
            //         minor_version,
            //         self.l1_verifier_config.recursion_scheduler_level_vk_hash,
            //     )
            //     .await
            //     .unwrap();
            // if allowed_patch_versions.is_empty() {
            //     tracing::warn!(
            //         "No patch version corresponds to the verification key on L1: {:?}",
            //         self.l1_verifier_config.recursion_scheduler_level_vk_hash
            //     );

            //     continue;
            // };

            // let allowed_versions: Vec<_> = allowed_patch_versions
            //     .into_iter()
            //     .map(|patch| ProtocolSemanticVersion {
            //         minor: minor_version,
            //         patch,
            //     })
            //     .collect();

            // let mut proof: Option<L1BatchProofForL1> = None;

            // for version in &allowed_versions {
            //     match self.blob_store.get((batch_to_prove, *version)).await {
            //         Ok(p) => {
            //             proof = Some(p);
            //             break;
            //         }
            //         Err(ObjectStoreError::KeyNotFound(_)) => (), // do nothing, proof is not ready yet
            //         Err(err) => panic!(
            //             "Failed to load proof for batch {}: {}",
            //             batch_to_prove.0, err
            //         ),
            //     }
            // }

            // if proof.is_none() {
            //     let is_patch_0_present = allowed_versions.iter().any(|v| v.patch.0 == 0);
            //     if is_patch_0_present {
            //         match self
            //             .blob_store
            //             .get_by_encoded_key(format!("l1_batch_proof_{batch_to_prove}.bin"))
            //             .await
            //         {
            //             Ok(p) => proof = Some(p),
            //             Err(ObjectStoreError::KeyNotFound(_)) => (), // do nothing, proof is not ready yet
            //             Err(err) => panic!(
            //                 "Failed to load proof for batch {}: {}",
            //                 batch_to_prove.0, err
            //             ),
            //         }
            //     }
            // }

            println!(
                "\n\n Batch to prove on ethereum:  {}\n Last batch: {}\n\n",
                batch_to_prove, previous_proven_batch_number
            );

            sleep(Duration::from_secs(1)).await;
            //self.
            // Optionally check for stop signal and break if received
            if *stop_receiver.0.borrow() {
                break;
            }
        }

        Ok(())
    }
}
