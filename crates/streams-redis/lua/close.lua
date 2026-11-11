-- Closes the Stream. A close writes no log entry, so it adds one entry to the `:closed` key, which
-- wakes each read that waits on the Stream.
--
-- ARGV: the `:closed` key, format version, kind, Producer id, the 40-byte epoch key, Tombstone ms,
-- `1` when a `WAIT` runs after it, the encoded Producer that closed the Stream.
--
-- Returns {'closed' or 'already'}.
local meta, log = KEYS[1], KEYS[2]
-- Keep `:closed` out of KEYS. It doesn't exist before the close, and while its slot migrates, a
-- script with a declared key that doesn't exist yet gets `TRYAGAIN`, so each close would fail.
-- Redis doesn't promise a script a key that KEYS doesn't name, and no script flag changes that.
-- It allows one of the script's own slot, and the slot migration case of `tests/acl.rs` checks it.
local closed_key, version, kind = ARGV[1], tonumber(ARGV[2]), ARGV[3]
local producer, epoch, tombstone = ARGV[4], ARGV[5], tonumber(ARGV[6])
local wait, closer = ARGV[7], ARGV[8]

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
if redis.call('HGET', meta, 'closed') then
  touch(meta, wait)
  return {'already'}
end
-- A Producer is fenced as its appends are, so a stale attempt can't close the Stream.
local stored, unknown = read_producer(meta, 'p|' .. kind .. '|' .. producer)
if unknown then
  return unknown
end
if stored and bytes_below(epoch, stored.fence) then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

-- The closer and the status let a read build the final StreamClose item. An SDK Store only
-- closes with CLOSED, which is STREAM_CLOSE_STATUS_CLOSED = 1.
redis.call('HSET', meta, 'closed', '1', 'closer', closer, 'cstat', '1')
redis.call('XADD', closed_key, '*', 'closed', '1')
-- No last-write TTL, since a closed Stream keeps its Retention.
refresh(meta, log, closed_key, now, nil, tombstone)
return {'closed'}
