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
/// The read scripts are read only, so a Reader runs them with `EVALSHA_RO`.
pub(crate) const READ: &str = concat!(
    "#!lua flags=no-writes\n",
    include_str!("../../lua/batch.lua"),
    include_str!("../../lua/read.lua")
);
pub(crate) const READ_BACK: &str = concat!(
    "#!lua flags=no-writes\n",
    include_str!("../../lua/batch.lua"),
    include_str!("../../lua/read_back.lua")
);

pub(crate) static CREATE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CREATE));
pub(crate) static APPEND_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(APPEND));
pub(crate) static CLOSE_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(CLOSE));
pub(crate) static READ_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(READ));
pub(crate) static READ_BACK_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(READ_BACK));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::documented_commands;
    use std::{collections::BTreeSet, path::Path};

    const WRITES: [&str; 3] = [CREATE, APPEND, CLOSE];
    const READS: [(&str, &str); 2] = [("read", READ), ("read_back", READ_BACK)];
    const READ_FLAG: &str = "#!lua flags=no-writes\n";

    fn lua_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("lua")
    }

    /// The Redis commands that a script calls, in lowercase. Each call names its command as a
    /// literal, so that this list is complete.
    fn calls(script: &str) -> BTreeSet<String> {
        script
            .match_indices("call(")
            .map(|(at, _)| {
                let rest = &script[at + "call(".len()..];
                let name = rest
                    .strip_prefix('\'')
                    .and_then(|rest| rest.split_once('\''));
                let (name, _) = name.unwrap_or_else(|| {
                    panic!("a script calls a command that isn't a literal: {rest:.40}")
                });
                name.to_lowercase()
            })
            .collect()
    }

    /// The commands of a role that the scripts must call, which are the documented ones with no
    /// command that the Store sends itself.
    fn script_commands(bullet: &str, sent: &[&str]) -> BTreeSet<String> {
        documented_commands(bullet)
            .into_iter()
            .filter(|command| !sent.contains(&command.as_str()))
            .collect()
    }

    #[test]
    fn each_lua_file_is_part_of_a_script() {
        let scripts: Vec<&str> = WRITES.into_iter().chain(READS.map(|(_, s)| s)).collect();
        for file in std::fs::read_dir(lua_dir()).unwrap() {
            let path = file.unwrap().path();
            let text = std::fs::read_to_string(&path).unwrap();
            assert!(
                scripts.iter().any(|script| script.contains(&text)),
                "{} is in no script of this module",
                path.display()
            );
        }
    }

    // A user that lacks a command fails a script after its first write, and that write stays. The
    // ACL lists also give no command that no script needs.
    #[test]
    fn each_role_s_scripts_call_exactly_its_documented_commands() {
        let writes = WRITES.iter().flat_map(|script| calls(script)).collect();
        let producer = script_commands("A Producer", &["evalsha", "script|load", "wait"]);
        assert_eq!(producer, writes, "the Producer list of docs/redis.md");
        let reads = READS.iter().flat_map(|(_, script)| calls(script)).collect();
        let reader = script_commands("A Reader", &["evalsha_ro", "script|load", "xread"]);
        assert_eq!(reader, reads, "the Reader list of docs/redis.md");
        // `EVALSHA_RO` runs only a script with the flag.
        assert!(
            READS
                .iter()
                .all(|(_, script)| script.starts_with(READ_FLAG))
        );
        assert!(WRITES.iter().all(|script| !script.starts_with("#!")));
    }

    // A Reader with no `+script|load` needs an admin to load each read script with the same SHA.
    #[test]
    fn the_docs_load_the_read_scripts_with_their_exact_bytes() {
        let docs = include_str!("../../docs/redis.md");
        let names = docs
            .split_once("for script in ")
            .and_then(|(_, rest)| rest.split_once("; do"))
            .map(|(names, _)| names.split_whitespace().collect::<Vec<_>>())
            .expect("docs/redis.md has the loop that loads the read scripts");
        assert_eq!(names, READS.map(|(name, _)| name));
        assert!(
            docs.contains(
                r#"printf '#!lua flags=no-writes\n'; cat lua/batch.lua "lua/$script.lua";"#
            )
        );
        let read = |name: &str| std::fs::read_to_string(lua_dir().join(name)).unwrap();
        for (name, script) in READS {
            let loaded = format!(
                "{READ_FLAG}{}{}",
                read("batch.lua"),
                read(&format!("{name}.lua"))
            );
            assert_eq!(loaded, script, "{name}");
        }
    }
}
