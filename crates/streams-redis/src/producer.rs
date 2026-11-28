//! Scripts compare opaque keys so that Producer encoding stays in the shared Streams crate.

use crate::error::{RedisStoreError, RedisStoreResult};
use sha2::{Digest, Sha256};
use temporalio_streams::{
    attempt::{ATTEMPT_KEY_LEN, attempt_key, producer_key},
    proto::Producer,
};

const REQUEST_KEY_LEN: usize = 16;

pub(crate) type AttemptKey = [u8; ATTEMPT_KEY_LEN];
pub(crate) type RequestKey = [u8; REQUEST_KEY_LEN];

// Attempts of one Producer must share its hash field.
pub(crate) fn producer_keys(producer: &Producer) -> RedisStoreResult<(Vec<u8>, AttemptKey)> {
    let field = producer_key(producer)
        .ok_or_else(|| RedisStoreError::Invalid("The Producer has no kind".to_string()))?;
    Ok((field, attempt_key(producer.kind.as_ref())))
}

// Fixed-size keys bound resend state. Random keys disable deduplication between calls
// without a request id while preserving protection for a transport retry of one call.
pub(crate) fn request_key(scope: &[u8], request_id: &str) -> RequestKey {
    if request_id.is_empty() {
        return rand::random();
    }
    let digest = Sha256::new()
        .chain_update(scope)
        .chain_update(request_id)
        .finalize();
    let mut key = [0; REQUEST_KEY_LEN];
    key.copy_from_slice(&digest[..REQUEST_KEY_LEN]);
    key
}
