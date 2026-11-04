//! The slot ranges of a Redis Cluster, as `CLUSTER SLOTS` gives them.

use redis::{FromRedisValue, ParsingError, Value};
use std::ops::RangeInclusive;

/// A node of a slot range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotNode {
    /// The host. It is empty for the node that answered, and `?` when the cluster doesn't know it.
    pub host: String,
    /// The port.
    pub port: u16,
    /// The node id.
    pub id: String,
}

/// A range of slots, with its primary and its replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotRange {
    /// The slots of the range.
    pub slots: RangeInclusive<u16>,
    /// The primary that serves the slots.
    pub primary: SlotNode,
    /// The replicas of the primary.
    pub replicas: Vec<SlotNode>,
}

fn next(fields: &mut impl Iterator<Item = Value>, what: &str) -> Result<Value, ParsingError> {
    fields
        .next()
        .ok_or_else(|| format!("CLUSTER SLOTS gave a {what} with too few fields").into())
}

/// A node is `[host, port, id, ...]`. The fields after the id, such as the hostname, don't matter
/// here.
impl FromRedisValue for SlotNode {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let fields: Vec<Value> = FromRedisValue::from_redis_value(value)?;
        let mut fields = fields.into_iter();
        Ok(Self {
            host: String::from_redis_value(next(&mut fields, "node")?)?,
            port: u16::from_redis_value(next(&mut fields, "node")?)?,
            id: String::from_redis_value(next(&mut fields, "node")?)?,
        })
    }
}

/// A range is `[start, end, primary, replica, ...]`.
impl FromRedisValue for SlotRange {
    fn from_redis_value(value: Value) -> Result<Self, ParsingError> {
        let fields: Vec<Value> = FromRedisValue::from_redis_value(value)?;
        let mut fields = fields.into_iter();
        let start = u16::from_redis_value(next(&mut fields, "range")?)?;
        let end = u16::from_redis_value(next(&mut fields, "range")?)?;
        let primary = SlotNode::from_redis_value(next(&mut fields, "range")?)?;
        Ok(Self {
            slots: start..=end,
            primary,
            replicas: fields
                .map(SlotNode::from_redis_value)
                .collect::<Result<_, _>>()?,
        })
    }
}
