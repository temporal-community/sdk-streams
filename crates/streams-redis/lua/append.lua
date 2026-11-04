-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, encoded run id, the 40-byte epoch key, sequence (0
-- takes the next one), encoded withdrawn run, Superseded facts, safety TTL ms, Tombstone ms, the
-- 24-byte task key, the completed task key (`-` when the write gives no task facts, empty for
-- none), failed task ids, task Superseded facts, the 16-byte batch digest, `1` when a `WAIT` runs
-- after it, the encoded Producer, then one Record with no Producer per argument.
--
-- Returns {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local run, epoch, sequence_text = ARGV[4], ARGV[5], ARGV[6]
local sequence = tonumber(sequence_text)
local withdrawn_run, superseded = ARGV[7], ARGV[8]
local safety, tombstone = tonumber(ARGV[9]), tonumber(ARGV[10])
local task, completed, failed, task_superseded = ARGV[11], ARGV[12], ARGV[13], ARGV[14]
local digest, wait, encoded_producer = ARGV[15], ARGV[16], ARGV[17]
local records = #ARGV - 17
local field = 'p|' .. kind .. '|' .. producer
local tracked = kind == '2' and run ~= ''
-- Only a Local Activity write with its run's task facts tells which tasks of the run completed.
local facts = tracked and completed ~= '-'
local open_field, gone_prefix, facts_field = 'oa|' .. run, 'ow|' .. run .. '|', 'tf|' .. run

if #epoch ~= EPOCH_KEY_LEN or #digest ~= DIGEST_LEN then
  return refuse('INVALID', 'the epoch key and the digest must be 40 and 16 bytes')
end
if tracked and #task ~= TASK_KEY_LEN then
  return refuse('INVALID', 'the task must be a task key')
end
if facts then
  if completed ~= '' and #completed ~= TASK_KEY_LEN then
    return refuse('INVALID', 'the completed task must be a task key')
  end
  -- A Lua number past 2^53 - 1 loses digits, so `started_key` can't pack such an id.
  for _, id in ipairs(words(failed)) do
    if not string.match(id, '^%-?%d+$') or math.abs(tonumber(id)) > 9007199254740991 then
      return refuse('INVALID', 'the failed tasks must be started event ids')
    end
  end
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
local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
-- Check the last batch before the close status, so that a resend gets its entry ids after a close.
if previous then
  if sequence == previous.sequence then
    if previous.digest ~= digest then
      return refuse('SEQ_DIVERGENT', 'sequence ' .. sequence_text ..
        ' has other content than the batch the Store has')
    end
    touch(meta, wait)
    return {'resend', previous.first, previous.last, ''}
  end
  if sequence < previous.sequence then
    return refuse('SEQUENCE', 'sequence ' .. sequence_text .. ' is below the last one, ' ..
      string.format('%.0f', previous.sequence))
  end
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
if stored and bytes_below(epoch, stored.fence) then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end
local newest_facts, open = nil, {}
if tracked then
  if redis.call('HGET', meta, gone_prefix .. task) then
    return refuse('FENCED', 'a Superseded item withdrew the Workflow Task of the call')
  end
  local value = redis.call('HGET', meta, facts_field)
  if value then
    newest_facts, unknown = task_keys(value)
    if unknown then
      return unknown
    end
    -- A task can fail before its call writes, so no orphan item withdrew it.
    local fenced
    fenced, unknown = fenced_by_facts(newest_facts, task)
    if unknown then
      return unknown
    end
    if fenced then
      return refuse('FENCED',
        "the run's task facts show that the Workflow Task of the call never completed")
    end
  end
  value = redis.call('HGET', meta, open_field)
  if value then
    open, unknown = task_keys(value)
    if unknown then
      return unknown
    end
  end
end

refused = check_ids_fit(meta, log, now, records + 2)
if refused then
  return refused
end

-- The first batch of a newer epoch withdraws the Records of the earlier ones. A client's (4)
-- newer epoch only fences, since its earlier Records are real messages.
local newer = kind ~= '4' and stored ~= nil and stored.held ~= nil and
  bytes_below(stored.held, epoch)
local flagged = newer
local withdrawn = {}
if facts then
  local completed_key = nil
  if completed ~= '' then
    completed_key = completed
  end
  withdrawn, open = orphan_tasks(open, task, completed_key, failed)
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
  orphan_id = add(log, {'s', task_superseded})
  last_id = orphan_id
end
if records == 0 and flagged then
  last_id = add(log, {'s', superseded})
  if first_id == '' then
    first_id = last_id
  end
end
-- Only the batch's first Record keeps the Producer. Each later one keeps the seq of that first
-- entry, so a read that starts inside the batch finds the Producer with one lookup.
local first_seq = next_seq
for i = 18, #ARGV do
  local fields = {'r', ARGV[i], 'b', first_seq}
  if i == 18 then
    fields = {'r', ARGV[i], 'p', encoded_producer}
    if flagged then
      table.insert(fields, 1, 's')
      table.insert(fields, 2, superseded)
    end
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

if first_id == '' then
  first_id = orphan_id
end

-- The batch's epoch holds Records when it wrote some. Only a newer epoch's item withdraws the
-- held epoch's Records, so any other empty batch keeps the held epoch as it was.
local held = nil
if records > 0 then
  held = epoch
elseif stored and not newer then
  held = stored.held
end
redis.call('HSET', meta, field, producer_value(epoch, held, sequence, first_id, last_id, digest))
-- Redis sends each write to the replicas and the AOF, so the script sets `run|` only once.
if run ~= '' and records > 0 and not redis.call('HGET', meta, 'run|' .. run) then
  redis.call('HSET', meta, 'run|' .. run, '1')
end
if tracked then
  -- A write with no task facts, as from an older Core, goes in `oa` too, so that a later write's
  -- facts can withdraw it.
  local added = records > 0 and not contains(open, task)
  if added then
    table.insert(open, task)
  end
  if facts or added then
    if #open > 0 then
      redis.call('HSET', meta, open_field, table.concat(open))
    else
      redis.call('HDEL', meta, open_field)
    end
  end
  -- One field per task, so the fence reads one field and a withdrawal writes only its new tasks.
  for _, key in ipairs(withdrawn) do
    redis.call('HSET', meta, gone_prefix .. key, '1')
  end
end
if facts then
  -- A long Local Activity of an earlier task writes with older facts.
  if not newest_facts or bytes_below(newest_facts[1], task) then
    local value = {task, completed}
    if completed == '' then
      value[2] = ZERO_KEY
    end
    for _, id in ipairs(words(failed)) do
      table.insert(value, started_key(tonumber(id)))
    end
    redis.call('HSET', meta, facts_field, table.concat(value))
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
