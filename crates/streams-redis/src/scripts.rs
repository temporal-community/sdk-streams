//! The Lua scripts of the Store, and a call of one.

use redis::{
    Cmd, ErrorKind, RedisResult, Script, ServerErrorKind, ToRedisArgs, Value, aio::ConnectionLike,
};
use std::sync::LazyLock;

const STATE: &str = include_str!("../lua/state.lua");
const WRITE: &str = include_str!("../lua/write.lua");

pub(crate) static CREATE: LazyLock<Script> =
    LazyLock::new(|| write_script(include_str!("../lua/create.lua")));
pub(crate) static APPEND: LazyLock<Script> =
    LazyLock::new(|| write_script(include_str!("../lua/append.lua")));
pub(crate) static CLOSE: LazyLock<Script> =
    LazyLock::new(|| write_script(include_str!("../lua/close.lua")));
pub(crate) static READ: LazyLock<Script> =
    LazyLock::new(|| read_script(include_str!("../lua/read.lua")));
pub(crate) static DESCRIBE: LazyLock<Script> =
    LazyLock::new(|| read_script(include_str!("../lua/describe.lua")));

fn write_script(body: &str) -> Script {
    Script::new(&[STATE, WRITE, body].concat())
}

/// Returns a script that `EVALSHA_RO` runs, so a Redis user with only read access to the keys can
/// call it.
fn read_script(body: &str) -> Script {
    Script::new(&["#!lua flags=no-writes\n", STATE, body].concat())
}

/// A call of a script, with its keys and arguments.
pub(crate) struct ScriptCall {
    script: &'static Script,
    cmd: Cmd,
}

impl ScriptCall {
    /// Returns a call of a script that writes.
    pub(crate) fn write(script: &'static Script, keys: &[String]) -> Self {
        Self::new("EVALSHA", script, keys)
    }

    /// Returns a call of a script that only reads.
    pub(crate) fn read(script: &'static Script, keys: &[String]) -> Self {
        Self::new("EVALSHA_RO", script, keys)
    }

    fn new(command: &str, script: &'static Script, keys: &[String]) -> Self {
        let mut cmd = redis::cmd(command);
        cmd.arg(script.get_hash()).arg(keys.len()).arg(keys);
        Self { script, cmd }
    }

    pub(crate) fn arg(&mut self, arg: impl ToRedisArgs) -> &mut Self {
        self.cmd.arg(arg);
        self
    }

    /// Runs the call. Redis forgets its scripts when it restarts, so a call that finds no script
    /// loads it and runs again. The first try wrote nothing, since the script didn't run.
    pub(crate) async fn run(&self, conn: &mut impl ConnectionLike) -> RedisResult<Value> {
        match self.cmd.query_async(conn).await {
            Err(error) if error.kind() == ErrorKind::Server(ServerErrorKind::NoScript) => {
                self.script.load_async(conn).await?;
                self.cmd.query_async(conn).await
            }
            result => result,
        }
    }
}
