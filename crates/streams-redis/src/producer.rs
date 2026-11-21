//! The keys that a script gets for the Producer of a write and for its request. The scripts
//! keep and compare them as opaque bytes.

use crate::error::{RedisStoreError, RedisStoreResult};
use sha2::{Digest, Sha256};
use temporalio_streams::{
    attempt::{ATTEMPT_KEY_LEN, attempt_key, producer_key},
    proto::Producer,
};

/// The length of a request key, in bytes.
const REQUEST_KEY_LEN: usize = 16;

pub(crate) type AttemptKey = [u8; ATTEMPT_KEY_LEN];
pub(crate) type RequestKey = [u8; REQUEST_KEY_LEN];

/// Returns the field of a Producer in the Producers hash, which is its [`producer_key`], and the
/// key of its attempt. Two attempts of a Producer have one field, and their keys order them.
pub(crate) fn producer_keys(producer: &Producer) -> RedisStoreResult<(Vec<u8>, AttemptKey)> {
    let field = producer_key(producer)
        .ok_or_else(|| RedisStoreError::Invalid("The Producer has no kind".to_string()))?;
    Ok((field, attempt_key(producer.kind.as_ref())))
}

/// Returns the request key of a call, which the Store keeps in place of the `request_id`. A
/// `request_id` is unique within `scope`. The key is the start of a SHA-256, so a long
/// `request_id` costs no more than a short one.
///
/// A call with no `request_id` gets a random key. No other call has it, so the call is never a
/// resend. The Redis client can still send the bytes of one call a second time, after a dropped
/// connection, and the script then finds the key and writes nothing.
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
