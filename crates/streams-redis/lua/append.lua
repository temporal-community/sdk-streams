-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, sequence (0 takes the next
-- one), encoded withdrawn run, Supersession bytes, safety TTL ms, Tombstone ms, task key, completed
-- task key (`-` when the write gives no task facts), failed task ids, task Supersession bytes, the
-- batch digest, `1` when a `WAIT` follows, then one Record per argument.
--
-- Returns {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local run, epoch, sequence_text = ARGV[4], ARGV[5], ARGV[6]
local sequence = tonumber(sequence_text)
local withdrawn_run, supersession = ARGV[7], ARGV[8]
local safety, tombstone = tonumber(ARGV[9]), tonumber(ARGV[10])
local task, completed, failed, task_supersession = ARGV[11], ARGV[12], ARGV[13], ARGV[14]
local digest, wait = ARGV[15], ARGV[16]
local records = #ARGV - 16
-- Only a Local Activity write with its run's task facts tells which tasks of the run completed.
local orphans = kind == '2' and completed ~= '-' and run ~= ''
local open_field, gone_field = 'oa|' .. run, 'ow|' .. run

-- The script reads the dedupe entry and writes the batch in one step, so no other batch takes the
-- same number.
if sequence_text == '0' then
  local last = redis.call('HGET', meta, 'd|' .. kind .. '|' .. producer) or ''
  local last_epoch, last_sequence = string.match(last, '^([^|]*)|(%d+)|')
  sequence_text = '1'
  if last_epoch == epoch then
    sequence_text = string.format('%.0f', tonumber(last_sequence) + 1)
  end
  sequence = tonumber(sequence_text)
end
-- A Lua number past 2^53 - 1 loses digits, so the dedupe entry keeps the sequence as sent.
if not string.match(sequence_text, '^[1-9]%d*$') or sequence > 9007199254740991 then
  return refuse('INVALID', 'the sequence must be from 1 to 2^53 - 1')
end
if orphans then
  if not task_key(task) or (completed ~= '' and not task_key(completed)) then
    return refuse('INVALID', 'the task and the completed task must be task keys')
  end
  for _, id in ipairs(words(failed)) do
    if not string.match(id, '^%-?%d+$') then
      return refuse('INVALID', 'the failed tasks must be started event ids')
    end
  end
end
local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
local suffix = '|' .. kind .. '|' .. producer
-- Check the dedupe key before the close status, so that a resend gets its entry ids after a close.
local last = redis.call('HGET', meta, 'd' .. suffix)
if last then
  local last_epoch, last_sequence, first_id, last_id, last_digest =
    string.match(last, '^([^|]*)|(%d+)|([^|]*)|([^|]*)|([^|]*)$')
  if last_epoch == epoch then
    last_sequence = tonumber(last_sequence)
    if sequence == last_sequence then
      if last_digest ~= digest then
        return refuse('SEQ_DIVERGENT', 'sequence ' .. sequence ..
          ' has other content than the batch the Store has')
      end
      touch(meta, wait)
      return {'resend', first_id, last_id, ''}
    end
    if sequence < last_sequence then
      return refuse('SEQUENCE', 'sequence ' .. sequence .. ' is below the last one, ' ..
        last_sequence)
    end
  end
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
local newest = redis.call('HGET', meta, 'e' .. suffix)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end
if kind == '2' and run ~= '' and contains(words(redis.call('HGET', meta, gone_field)), task) then
  return refuse('FENCED', 'a Superseded item withdrew the Workflow Task of the call')
end

refused = check_ids_fit(meta, log, now, records + 2)
if refused then
  return refused
end

-- The first batch of a newer epoch withdraws the Records of the earlier ones. A client's (4)
-- newer epoch only fences, since its earlier Records are real messages.
local holds = redis.call('HGET', meta, 'h' .. suffix)
local flagged = kind ~= '4' and holds ~= false and holds < epoch
local withdrawn, open = {}, {}
if orphans then
  withdrawn, open = orphan_tasks(words(redis.call('HGET', meta, open_field)), task, completed,
    failed)
end

-- `prune` reads `:producers` first, so a key of another type there fails before any write.
prune(meta, producers, now)
if withdraw_run(meta, withdrawn_run) then
  flagged = true
end
trim(meta, log, now)
local first_id, last_id = '', ''
-- The orphan item has an entry of its own, since the batch's first entry can have its own
-- Superseded item.
local orphan_id = ''
if #withdrawn > 0 then
  orphan_id = add(log, {'s', task_supersession})
  last_id = orphan_id
end
if records == 0 and flagged then
  last_id = add(log, {'s', supersession})
  if first_id == '' then
    first_id = last_id
  end
end
for i = 17, #ARGV do
  local fields = {'r', ARGV[i]}
  if i == 17 and flagged then
    fields = {'s', supersession, 'r', ARGV[i]}
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

if first_id == '' then
  first_id = orphan_id
end

redis.call('HSET', meta, 'e' .. suffix, epoch)
redis.call('HSET', meta, 'd' .. suffix,
  epoch .. '|' .. sequence_text .. '|' .. first_id .. '|' .. last_id .. '|' .. digest)
if records > 0 then
  redis.call('HSET', meta, 'h' .. suffix, epoch)
elseif flagged then
  redis.call('HDEL', meta, 'h' .. suffix)
end
-- Each write goes to the replicas and the AOF, so `run|` is set once.
if run ~= '' and records > 0 and not redis.call('HGET', meta, 'run|' .. run) then
  redis.call('HSET', meta, 'run|' .. run, '1')
end
if orphans then
  if records > 0 and not contains(open, task) then
    table.insert(open, task)
  end
  if #open > 0 then
    redis.call('HSET', meta, open_field, table.concat(open, ' '))
  else
    redis.call('HDEL', meta, open_field)
  end
  if #withdrawn > 0 then
    local all = words(redis.call('HGET', meta, gone_field))
    for _, key in ipairs(withdrawn) do
      table.insert(all, key)
    end
    redis.call('HSET', meta, gone_field, table.concat(all, ' '))
  end
end
if last_id ~= '' then
  redis.call('HSET', meta, 'newest', last_id)
end
redis.call('ZADD', producers, next_ms, kind .. '|' .. producer)
refresh(meta, log, producers, now, safety, tombstone)
local flag = ''
if flagged or #withdrawn > 0 then
  flag = '1'
end
return {'ok', first_id, last_id, flag}
