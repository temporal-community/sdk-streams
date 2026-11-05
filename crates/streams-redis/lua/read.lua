-- Reads the Stream's state and up to ARGV[2] log entries from the id ARGV[1], inclusive, in one
-- step, so the entries and the state agree.
--
-- Returns {state, server time, entries, producer}. The state is the meta fields v, closed,
-- close_id, newest, trimmed, ret and exp, with false for a missing one. The producer is the
-- encoded Producer of the batch that the first entry is part of, when that batch starts earlier.
local meta, log = KEYS[1], KEYS[2]
local state = redis.call('HMGET', meta, 'v', 'closed', 'newest', 'trimmed', 'ret', 'exp')
local entries = {}
if tonumber(ARGV[2]) > 0 then
  entries = redis.call('XRANGE', log, ARGV[1], '+', 'COUNT', ARGV[2])
end
local function field(fields, name)
  for i = 1, #fields, 2 do
    if fields[i] == name then
      return fields[i + 1]
    end
  end
end

-- A batch's later entries keep the seq of its first entry, which keeps the Producer.
local producer = ''
local start = entries[1] and field(entries[1][2], 'b')
if start then
  local id = string.match(entries[1][1], '^%d+-') .. start
  local batch = redis.call('XRANGE', log, id, id)[1]
  producer = batch and field(batch[2], 'p') or ''
end
return {state, redis.call('TIME'), entries, producer}
