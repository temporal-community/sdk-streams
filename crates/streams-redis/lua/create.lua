-- Creates the Stream's meta key, or finds it. It refuses a Stream that is expired, has another
-- owner or is closed.
--
-- ARGV: format version, Retention ms, owner (`wf` or `client`), safety TTL ms, Tombstone ms, the
-- owner's run chain as `<workflow id>:<first run id>` with each part encoded (empty for a client),
-- `1` when a `WAIT` follows.
--
-- Returns {outcome, stored Retention ms}. The outcome is `created` or `exists`. A Stream that
-- exists keeps its own Retention.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local version, retention, owner = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local safety, tombstone = tonumber(ARGV[4]), tonumber(ARGV[5])
local chain, wait = ARGV[6], ARGV[7]

local now = now_ms()
local stored = redis.call('HGET', meta, 'ret')
if not stored then
  redis.call('HSET', meta, 'v', version, 'ret', retention, 'own', owner)
  if chain ~= '' then
    redis.call('HSET', meta, 'oc', chain)
  end
  refresh(meta, log, producers, now, safety, tombstone)
  return {'created', retention}
end
local refused = check_live(meta, version, now) or check_identity(meta, owner, chain)
if refused then
  return refused
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
touch(meta, wait)
return {'exists', stored}
