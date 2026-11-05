-- Closes the Stream. A close writes no log entry, so it adds one entry to the `:closed` key, which
-- wakes each read that waits on the Stream.
--
-- ARGV: the `:closed` key, format version, kind, Producer id, encoded run id, the 40-byte epoch
-- key, Tombstone ms, the run chain that the close's ref gives as `<workflow id>:<first run id>`
-- with each part encoded (empty for none), `1` when a `WAIT` runs after it.
--
-- Returns {'closed' or 'already'}.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
-- Keep `:closed` out of KEYS. It doesn't exist before the close, and while its slot migrates, a
-- script with a declared key that doesn't exist yet gets `TRYAGAIN`, so each close would fail.
-- Redis doesn't promise a script a key that KEYS doesn't name, and no script flag changes that.
-- It allows one of the script's own slot, and the slot migration case of `tests/acl.rs` checks it.
local closed_key, version, kind = ARGV[1], tonumber(ARGV[2]), ARGV[3]
local producer, run, epoch, tombstone = ARGV[4], ARGV[5], ARGV[6], tonumber(ARGV[7])
local chain, wait = ARGV[8], ARGV[9]

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
refused = check_owner(meta, kind, chain)
if refused then
  return refused
end
if redis.call('HGET', meta, 'closed') then
  touch(meta, wait)
  return {'already'}
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
-- A Producer is fenced as its appends are, so a stale attempt can't close the Stream.
local stored, unknown = read_producer(meta, 'p|' .. kind .. '|' .. producer)
if unknown then
  return unknown
end
if stored and bytes_below(epoch, stored.fence) then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

redis.call('HSET', meta, 'closed', '1')
redis.call('XADD', closed_key, '*', 'closed', '1')
-- Safety TTL 0, since a closed Stream keeps its Retention.
refresh(meta, log, producers, closed_key, now, 0, tombstone)
return {'closed'}
