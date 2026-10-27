//! The generated config of the Redis runtime.

#![allow(missing_docs, unreachable_pub, clippy::all)]

pub(crate) mod temporal {
    pub(crate) mod sdk {
        pub(crate) mod streams {
            pub(crate) mod v1 {
                include!(concat!(env!("OUT_DIR"), "/temporal.sdk.streams.v1.rs"));
            }
        }
    }
}
