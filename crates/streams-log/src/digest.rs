//! The content digest of a batch. A dedupe entry stores the digest of its batch, so that a resend
//! with the same `sequence` and other content fails instead of being dropped without an error. The
//! Store compares the digest that [batch_digest] gives, and `testdata/digests.json` has its
//! expected values.

use sha2::{Digest, Sha256};
use temporalio_common::protos::temporal::api::common::v1::Payload;
use temporalio_streams::proto::AppendRecord;

/// The length of a [batch_digest].
pub const DIGEST_LEN: usize = 16;

/// The first 16 bytes of a SHA-256 over the Records in order. Each part has its length before it,
/// so that two different batches never hash the same bytes. The metadata is hashed in key order,
/// since a map has no order on the wire.
pub fn batch_digest(records: &[AppendRecord]) -> [u8; DIGEST_LEN] {
    let mut hasher = Sha256::new();
    for record in records {
        body(&mut hasher, record.body.as_ref());
    }
    finish(hasher)
}

fn body(hasher: &mut Sha256, body: Option<&Payload>) {
    // A Record with no body and one with an empty body are two different stored Records.
    let Some(body) = body else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let mut metadata: Vec<_> = body.metadata.iter().collect();
    metadata.sort();
    hasher.update((metadata.len() as u64).to_be_bytes());
    for (key, value) in metadata {
        part(hasher, key.as_bytes());
        part(hasher, value);
    }
    part(hasher, &body.data);
    hasher.update((body.external_payloads.len() as u64).to_be_bytes());
    for external in &body.external_payloads {
        hasher.update(external.size_bytes.to_be_bytes());
    }
}

fn finish(hasher: Sha256) -> [u8; DIGEST_LEN] {
    let mut digest = [0; DIGEST_LEN];
    digest.copy_from_slice(&hasher.finalize()[..DIGEST_LEN]);
    digest
}

fn part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}
