//! The Lua scripts, one per operation. They are the canonical form of the stored format, so a
//! change to a script is a change of the format.

use redis::Script;
use std::sync::LazyLock;

pub(crate) const CREATE: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/create.lua")
);
pub(crate) const APPEND: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/append.lua")
);
pub(crate) const CLOSE: &str = concat!(
    include_str!("../../lua/lib.lua"),
    include_str!("../../lua/close.lua")
);
/// Read only, so it also runs on a replica.
pub(crate) const READ: &str = concat!(
    "#!lua flags=no-writes\n",
    include_str!("../../lua/read.lua")
);

pub(crate) static CREATE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CREATE));
pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CLOSE));
pub(crate) static READ_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(READ));
