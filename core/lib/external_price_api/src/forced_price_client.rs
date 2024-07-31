use std::{
    cmp::{max, min},
    num::NonZeroU64,
};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::RwLock;
use zksync_config::configs::ExternalPriceApiClientConfig;
use zksync_types::{base_token_ratio::BaseTokenAPIRatio, Address};

use crate::PriceAPIClient;

const VARIATION_RANGE: f64 = 0.2; //20%
const NEXT_VALUE_VARIATION_RANGE: f64 = 0.03; //3%

// Struct for a forced price "client" (conversion ratio is always a configured "forced" ratio).
#[derive(Debug)]
pub struct ForcedPriceClient {
    ratio: BaseTokenAPIRatio,
    previous_numerator: RwLock<NonZeroU64>,
}

impl ForcedPriceClient {
    pub fn new(config: ExternalPriceApiClientConfig) -> Self {
        let numerator = config
            .forced_numerator
            .expect("forced price client started with no forced numerator");
        let denominator = config
            .forced_denominator
            .expect("forced price client started with no forced denominator");

        Self {
            ratio: BaseTokenAPIRatio {
                numerator: NonZeroU64::new(numerator).unwrap(),
                denominator: NonZeroU64::new(denominator).unwrap(),
                ratio_timestamp: chrono::Utc::now(),
            },
            previous_numerator: RwLock::new(NonZeroU64::new(numerator).unwrap()),
        }
    }
}

#[async_trait]
impl PriceAPIClient for ForcedPriceClient {
    /// Returns a ratio which is 10% higher or lower than the configured forced ratio,
    /// but not different more than 3% than the last value
    async fn fetch_ratio(&self, _token_address: Address) -> anyhow::Result<BaseTokenAPIRatio> {
        let mut previous_numerator = self.previous_numerator.write().await;
        let mut rng = rand::thread_rng();
        let numerator_range = (
            max(
                (self.ratio.numerator.get() as f64 * (1.0 - VARIATION_RANGE)).round() as u64,
                (previous_numerator.get() as f64 * (1.0 - NEXT_VALUE_VARIATION_RANGE)).round()
                    as u64,
            ),
            min(
                (self.ratio.numerator.get() as f64 * (1.0 + VARIATION_RANGE)).round() as u64,
                (previous_numerator.get() as f64 * (1.0 + NEXT_VALUE_VARIATION_RANGE)).round()
                    as u64,
            ),
        );

        let new_numerator = NonZeroU64::new(rng.gen_range(numerator_range.0..=numerator_range.1))
            .unwrap_or(self.ratio.numerator);
        let adjusted_ratio = BaseTokenAPIRatio {
            numerator: new_numerator,
            denominator: self.ratio.denominator,
            ratio_timestamp: chrono::Utc::now(),
        };
        *previous_numerator = new_numerator;

        Ok(adjusted_ratio)
    }
}