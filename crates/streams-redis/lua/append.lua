-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, the 40-byte epoch key, sequence (0 takes the next one),
-- last-write TTL ms (empty for none), Tombstone ms, the 16-byte batch digest, `1` when a `WAIT`
-- runs after it, the encoded Producer, then one Record with no Producer per argument.
--
-- Returns {'ok' or 'resend', first id, last id}. A batch that wrote no entry gives '' for both ids.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local epoch, sequence_text = ARGV[4], ARGV[5]
local sequence = tonumber(sequence_text)
local last_write_ttl, tombstone = tonumber(ARGV[6]), tonumber(ARGV[7])
local digest, wait, encoded_producer = ARGV[8], ARGV[9], ARGV[10]
local records = #ARGV - 10
local field = 'p|' .. kind .. '|' .. producer

if #epoch ~= EPOCH_KEY_LEN or #digest ~= DIGEST_LEN then
  return refuse('INVALID', 'the epoch key and the digest must be 40 and 16 bytes')
end
-- The format version comes first, since a newer format can change how the Producer field reads.
local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
local stored, unknown = read_producer(meta, field)
if unknown then
  return unknown
end
-- Each append sets the fence to its epoch, so the fence is also the epoch of the last batch.
local previous = nil
if stored and stored.fence == epoch then
  previous = stored
end
-- The script reads the last batch and writes this one in one step, so no other batch takes the
-- same number.
if sequence_text == '0' then
  sequence = 1
  if previous then
    sequence = previous.sequence + 1
  end
  sequence_text = string.format('%.0f', sequence)
end
-- A Lua number past 2^53 - 1 loses digits.
if not string.match(sequence_text, '^[1-9]%d*$') or sequence > 9007199254740991 then
  return refuse('INVALID', 'the sequence must be from 1 to 2^53 - 1')
end
-- Check the last batch before the close status, so that a resend gets its entry ids after a close.
if previous then
  if sequence == previous.sequence then
    if previous.digest ~= digest then
      return refuse('SEQ_DIVERGENT', 'sequence ' .. sequence_text ..
        ' has other content than the batch the Store has')
    end
    touch(meta, wait)
    return {'resend', previous.first, previous.last}
  end
  if sequence < previous.sequence then
    return refuse('SEQUENCE_TOO_LOW', 'sequence ' .. sequence_text .. ' is below the last one, ' ..
      string.format('%.0f', previous.sequence))
  end
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
if stored and bytes_below(epoch, stored.fence) then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

refused = check_ids_fit(meta, log, now, records)
if refused then
  return refused
end

mark_log_gone(meta, log)
local first_id, last_id = '', ''
-- Only the batch's first Record keeps the Producer. Each later one keeps the seq of that first
-- entry, so a read that starts inside the batch finds the Producer with one lookup.
local first_seq = next_seq
for i = 11, #ARGV do
  local fields = {'r', ARGV[i], 'b', first_seq}
  if i == 11 then
    fields = {'r', ARGV[i], 'p', encoded_producer}
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

redis.call('HSET', meta, field, producer_value(epoch, sequence, first_id, last_id, digest))
if last_id ~= '' then
  redis.call('HSET', meta, 'newest', last_id)
end
refresh(meta, log, nil, now, last_write_ttl, tombstone)
return {'ok', first_id, last_id}
