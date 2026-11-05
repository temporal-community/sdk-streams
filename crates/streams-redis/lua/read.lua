-- Reads the Stream's state and up to ARGV[2] log entries from the id ARGV[1], inclusive, in one
-- step, so the entries and the state agree.
--
-- Returns {state, server time, entries, producer}. The state is the meta fields v, closed, newest,
-- trimmed, ret and exp, with false for a missing one. The producer is the encoded Producer of the
-- batch that the first entry is part of, when that batch starts earlier.
local meta, log = KEYS[1], KEYS[2]
local state = redis.call('HMGET', meta, 'v', 'closed', 'newest', 'trimmed', 'ret', 'exp')
local entries = {}
if tonumber(ARGV[2]) > 0 then
  entries = redis.call('XRANGE', log, ARGV[1], '+', 'COUNT', ARGV[2])
end
return {state, redis.call('TIME'), entries, batch_producer(log, entries[1])}
