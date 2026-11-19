//! The shared test vectors, which `examples/gen_vectors.rs` writes from a second implementation.
//! This file reads `ids.json`, `offsets.json` and `digests.json`. The unit tests of the field
//! layout read `fields.json`.

use serde_json::Value;
use temporalio_common::protos::temporal::api::common::v1::{
    Payload, payload::ExternalPayloadDetails,
};
use temporalio_streams::proto::AppendRecord;
use temporalio_streams_log::{
    digest::batch_digest,
    ids::StreamKeys,
    offset::{entry_id_of_offset, offset_of_entry_id},
};

fn load(name: &str) -> Value {
    let path = format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn number(value: &Value, field: &str) -> i64 {
    value[field].as_i64().unwrap_or(0)
}

#[test]
fn stream_ids_and_keys_match() {
    let vectors = load("ids.json");
    for case in vectors["streams"].as_array().unwrap() {
        let keys = StreamKeys::new(text(case, "prefix"), text(case, "stream_id"));
        assert_eq!(keys.meta(), text(case, "meta"));
        assert_eq!(keys.log(), text(case, "log"));
        assert_eq!(keys.closed(), text(case, "closed"));
    }
}

#[test]
fn offsets_match() {
    for case in load("offsets.json").as_array().unwrap() {
        let offset = number(case, "offset");
        assert_eq!(offset_of_entry_id(text(case, "entry_id")).unwrap(), offset);
        assert_eq!(entry_id_of_offset(offset), text(case, "entry_id"));
    }
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// The body of a vector Record. A Record with none of these keys has no body.
fn body(r: &Value) -> Option<Payload> {
    ["metadata", "data", "external_payloads"]
        .iter()
        .any(|key| r.get(key).is_some())
        .then(|| Payload {
            metadata: r["metadata"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(key, value)| (key.clone(), hex(value.as_str().unwrap())))
                .collect(),
            data: hex(r["data"].as_str().unwrap_or_default()),
            external_payloads: r["external_payloads"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|size| ExternalPayloadDetails {
                    size_bytes: size.as_i64().unwrap(),
                })
                .collect(),
        })
}

#[test]
fn batch_digests_match() {
    for case in load("digests.json").as_array().unwrap() {
        let records: Vec<_> = case["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| AppendRecord { body: body(r) })
            .collect();
        assert_eq!(
            batch_digest(&records).to_vec(),
            hex(text(case, "digest")),
            "{}",
            text(case, "name")
        );
    }
}
