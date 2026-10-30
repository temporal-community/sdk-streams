-- Closes the Stream with one close Record.
--
-- ARGV: format version, kind, Producer id, epoch key, Tombstone ms, close Record, the run chain
-- that the close's ref names as `<workflow id>:<first run id>` with each part encoded (empty for
-- none), `1` when a `WAIT` follows.
--
-- Returns {'closed' or 'already', close id}.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local epoch, tombstone, record = ARGV[4], tonumber(ARGV[5]), ARGV[6]
local chain, wait = ARGV[7], ARGV[8]

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
  return {'already', redis.call('HGET', meta, 'close_id')}
end
-- A Producer is fenced as its appends are, so a stale attempt can't close the Stream.
local newest = redis.call('HGET', meta, 'e|' .. kind .. '|' .. producer)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

refused = check_ids_fit(meta, log, now, 1)
if refused then
  return refused
end

trim(meta, log, now)
local id = add(log, {'r', record})
redis.call('HSET', meta, 'closed', '1', 'close_id', id, 'newest', id)
-- Safety TTL 0, since a closed Stream keeps its Retention.
refresh(meta, log, producers, now, 0, tombstone)
return {'closed', id}
