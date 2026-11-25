-- Appends one batch of one Producer.
--
-- KEYS: the meta key, the log, the Producers.
-- ARGV:
-- 1. The field of the Producer
-- 2. The attempt key of the batch (28 bytes)
-- 3. The request key of the batch (16 bytes)
-- 4. `1` when the Store fences
-- 5. The number of batches that the Store remembers for a Producer
-- 6. `1` when a `WAIT` comes after the script
-- 7. The encoded Producer
-- 8. The encoded body of each Record, one argument each. A batch has one Record or more.
--
-- Returns the `<ms>` of the batch's Records, and the `<seq>` of the first one and of the last one.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local field, attempt, request = ARGV[1], ARGV[2], ARGV[3]
local fence, window, wait, producer = ARGV[4], tonumber(ARGV[5]), ARGV[6], ARGV[7]
local FIRST_RECORD = 8
-- A remembered batch is its request key and the three numbers of its answer, 8 bytes each.
local REQUEST, BATCH = 16, 40

local now = now_ms()
local stream, refused = live(meta, now)
if refused then
  return refused
end
-- A `ret` that isn't a number fails the script here, before its first write.
local expires = now + stream.ret
local state = redis.call('HGET', producers, field) or ''
local newest_attempt, batches = string.sub(state, 1, ATTEMPT), string.sub(state, ATTEMPT + 1)
-- A resend comes before the fence, so that a retry gets its answer after a later attempt wrote.
for at = 1, #batches, BATCH do
  if string.sub(batches, at, at + REQUEST - 1) == request then
    local batch_ms, first_seq, last_seq = struct.unpack('>I8I8I8', batches, at + REQUEST)
    touch(meta, wait)
    return {batch_ms, first_seq, last_seq}
  end
end
if fence == '1' and below(attempt, newest_attempt) then
  return refuse('FENCED', 'A later attempt of the Producer already wrote to the Stream')
end

local lost = lost_mark(stream, log)
if lost and lost ~= stream.lost then
  redis.call('HSET', meta, 'lost', lost)
end
-- The ids of a batch have one `<ms>`. They start after `newest`, so they never go down when
-- the clock steps back or the log is lost.
local ms, seq = id_parts(stream.newest)
if now > ms then
  ms, seq = now, 0
else
  seq = seq + 1
end
local function id(entry_seq)
  return string.format('%.0f-%.0f', ms, entry_seq)
end
local first_seq = seq
redis.call('XADD', log, id(seq), 'p', producer, 'r', ARGV[FIRST_RECORD])
for i = FIRST_RECORD + 1, #ARGV do
  seq = seq + 1
  redis.call('XADD', log, id(seq), 'b', first_seq, 'r', ARGV[i])
end
redis.call('HSET', meta, 'newest', id(seq))

if below(newest_attempt, attempt) then
  newest_attempt = attempt
end
local batch = request .. struct.pack('>I8I8I8', ms, first_seq, seq)
redis.call('HSET', producers, field,
  newest_attempt .. batch .. string.sub(batches, 1, (window - 1) * BATCH))
-- A resend writes no batch, so only this path starts the Retention again.
expire_at(meta, {log, producers}, expires)
return {ms, first_seq, seq}
