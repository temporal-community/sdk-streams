-- Closes a Stream. A close stores no Record, so it adds an entry to the `closed` key, which
-- wakes each read that waits on the Stream.
--
-- KEYS: the meta key, the log, the Producers, the `closed` key.
-- ARGV:
-- 1. The field of the Producer
-- 2. The attempt key of the close (28 bytes)
-- 3. `1` when the Store fences
-- 4. `1` when a `WAIT` comes after the script
--
-- Returns 1.
local meta, log, producers, closed = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local field, attempt, fence, wait = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

local now = now_ms()
local stream, refused = live(meta, now)
if refused then
  return refused
end
if stream.closed then
  touch(meta, wait)
  return 1
end
if fence == '1' then
  local state = redis.call('HGET', producers, field)
  if state and below(attempt, string.sub(state, 1, ATTEMPT)) then
    return refuse('FENCED', 'A later attempt of the Producer already wrote to the Stream')
  end
end
redis.call('HSET', meta, 'closed', 1)
redis.call('XADD', closed, '*', 'c', 1)
expire_at(meta, {log, producers, closed}, now + stream.ret)
return 1
