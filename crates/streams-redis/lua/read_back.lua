-- Reads up to ARGV[2] log entries at or below the id ARGV[1], newest first, in one step.
--
-- Returns {entries, producer}. The producer is the encoded Producer of the batch that the oldest
-- entry is part of, when that batch starts earlier.
local log = KEYS[1]
local entries = {}
if tonumber(ARGV[2]) > 0 then
  entries = redis.call('XREVRANGE', log, ARGV[1], '-', 'COUNT', ARGV[2])
end
return {entries, batch_producer(log, entries[#entries])}
