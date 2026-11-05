-- The prelude of every script. KEYS[1] is the Stream's meta hash, KEYS[2] its log, and KEYS[3] the
-- sorted set of its Producers' last appends. Each script checks everything that can refuse the call
-- before its first write, since Redis doesn't roll a script back.

local function now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

local function refuse(code, message)
  return redis.error_reply('STREAM_' .. code .. ' ' .. message)
end

-- Returns nil when the Stream accepts writes, or else the refusal. An open Stream whose `exp`
-- passed is a Tombstone, and only a read still uses it.
local function check_live(meta, version, now)
  local v = redis.call('HGET', meta, 'v')
  if not v then
    return refuse('NOT_FOUND', 'nobody created the Stream')
  end
  if tonumber(v) > version then
    return refuse('FORMAT_TOO_NEW', 'format ' .. v .. ' is newer than ' .. version)
  end
  local exp = tonumber(redis.call('HGET', meta, 'exp'))
  if exp and exp <= now then
    return refuse('EXPIRED', 'the Stream expired')
  end
  return nil
end

-- Returns nil when the Producer kind may close the Stream, or else the refusal. Only the Workflow
-- Producer (1) closes a Workflow-owned Stream, with a handle of the run chain in `oc`. A Local
-- Activity (2) can't, since it writes before its task commits. A Workflow or a client (4) closes a
-- Stream with no owner.
local function check_owner(meta, kind, chain)
  if redis.call('HGET', meta, 'own') == 'wf' then
    if kind ~= '1' then
      return refuse('NOT_OWNER', 'only the close Activity of the owner run chain closes a ' ..
        'Workflow-owned Stream')
    end
    local owner_chain = redis.call('HGET', meta, 'oc')
    if owner_chain and chain ~= owner_chain then
      return refuse('NOT_OWNER', 'the handle names another run chain than the owner')
    end
  elseif kind ~= '1' and kind ~= '4' then
    return refuse('NOT_OWNER', 'only a Workflow or a client closes a Stream with no owner')
  end
  return nil
end

-- Returns nil when a create gives the Stream's owner, or else the refusal. The owner is part of
-- the Stream's identity, so a create can't change it.
local function check_identity(meta, owner, chain)
  local stored = redis.call('HGET', meta, 'oc') or ''
  if redis.call('HGET', meta, 'own') ~= owner or stored ~= chain then
    return refuse('NOT_OWNER', 'the Stream has another owner than the create names')
  end
  return nil
end

-- Sets the expiry after this write. It's the safety TTL for an open Workflow-owned Stream, or the
-- Retention otherwise. The meta key expires 30 days later, as the Tombstone. Only a close gives the
-- `closed` key, since a closed Stream takes no other write.
local function refresh(meta, log, producers, closed, now, safety, tombstone)
  local retention = tonumber(redis.call('HGET', meta, 'ret'))
  local exp = now + retention
  if redis.call('HGET', meta, 'own') == 'wf' and not redis.call('HGET', meta, 'closed') then
    exp = now + safety
  end
  redis.call('HSET', meta, 'exp', exp)
  redis.call('PEXPIREAT', meta, exp + tombstone)
  for _, key in ipairs({producers, closed}) do
    if redis.call('EXISTS', key) == 1 then
      redis.call('PEXPIREAT', key, exp)
    end
  end
  -- A read hides every Record one Retention after the last write. `newest` keeps later ids above
  -- the expired ones.
  if redis.call('EXISTS', log) == 1 then
    redis.call('PEXPIREAT', log, math.min(exp, now + retention))
  end
end

-- Before a `WAIT`, a call that finds its write done writes `v` again. From Redis 7.2, a `WAIT`
-- after a script that wrote nothing returns at once, though the replicas may lack that write.
local function touch(meta, wait)
  if wait == '1' then
    redis.call('HSET', meta, 'v', redis.call('HGET', meta, 'v'))
  end
end

-- Whether entry id a is below entry id b.
local function id_below(a, b)
  local a_ms, a_seq = string.match(a, '^(%d+)-(%d+)$')
  local b_ms, b_seq = string.match(b, '^(%d+)-(%d+)$')
  a_ms, b_ms = tonumber(a_ms), tonumber(b_ms)
  return a_ms < b_ms or (a_ms == b_ms and tonumber(a_seq) < tonumber(b_seq))
end

-- Sets `trimmed` to the entry id `id`, unless it is at or past it already. A clock that steps
-- back must not lower the watermark.
local function raise_trimmed(meta, id)
  local trimmed = redis.call('HGET', meta, 'trimmed')
  if not trimmed or id_below(trimmed, id) then
    redis.call('HSET', meta, 'trimmed', id)
  end
end

-- Drops the Records older than the Retention and keeps the newest dropped id, so a read can
-- tell expired Records from an empty Stream.
local function trim(meta, log, now)
  if redis.call('EXISTS', log) == 0 then
    -- Every Record expired, so a read from an older offset must fail.
    local newest = redis.call('HGET', meta, 'newest')
    if newest then
      raise_trimmed(meta, newest)
    end
    return
  end
  local min = now - tonumber(redis.call('HGET', meta, 'ret'))
  if min <= 0 then
    return
  end
  local old = redis.call('XREVRANGE', log, '(' .. min .. '-0', '-', 'COUNT', 1)
  if #old > 0 then
    raise_trimmed(meta, old[1][1])
    redis.call('XTRIM', log, 'MINID', min)
  end
end

-- The most Producers that one append prunes, so that an append after a long gap stays short.
local PRUNE_LIMIT = 64

-- Removes the field of each Producer whose last append is older than the Retention. Its Records
-- are older too, so the log has none of them.
local function prune(meta, producers, now)
  local min = now - tonumber(redis.call('HGET', meta, 'ret'))
  local old = redis.call('ZRANGEBYSCORE', producers, '-inf', '(' .. min, 'LIMIT', 0, PRUNE_LIMIT)
  for _, producer in ipairs(old) do
    redis.call('HDEL', meta, 'p|' .. producer)
  end
  if #old > 0 then
    redis.call('ZREM', producers, unpack(old))
  end
end

local function contains(list, value)
  for _, item in ipairs(list) do
    if item == value then
      return true
    end
  end
  return false
end

-- A run that never wrote here needs the fence but no Superseded item.
local function withdraw_run(meta, run)
  if run == '' or redis.call('HGET', meta, 'wr|' .. run) then
    return false
  end
  redis.call('HSET', meta, 'wr|' .. run, '1')
  return redis.call('HGET', meta, 'run|' .. run) ~= false
end

-- The id of the script's next entry. A script numbers its entries itself, in one millisecond,
-- since `XADD *` on Redis 7.0 can cross a millisecond inside one script.
local next_ms, next_seq = 0, 0

-- Starts the script's ids after the newest id the log had, which `newest` keeps after a trim.
-- Refuses `count` entries whose ids would not fit an offset, which packs the sequence in 20 bits.
local function check_ids_fit(meta, log, now, count)
  local newest = redis.call('HGET', meta, 'newest')
  local entry = redis.call('XREVRANGE', log, '+', '-', 'COUNT', 1)
  if #entry > 0 and (not newest or id_below(newest, entry[1][1])) then
    newest = entry[1][1]
  end
  next_ms, next_seq = now, 0
  if newest then
    local ms, seq = string.match(newest, '^(%d+)-(%d+)$')
    if tonumber(ms) >= now then
      next_ms, next_seq = tonumber(ms), tonumber(seq) + 1
    end
  end
  if next_seq + count > 1048576 then
    return refuse('STORAGE_UNAVAILABLE',
      'the entry ids of this millisecond would not fit an offset')
  end
  return nil
end

-- Adds an entry with the script's next id. A script calls `check_ids_fit` before its first one.
local function add(log, fields)
  local id = string.format('%d-%d', next_ms, next_seq)
  next_seq = next_seq + 1
  return redis.call('XADD', log, id, unpack(fields))
end

-- Whether the binary key a sorts below b. Lua's `<` collates with the server's locale, so it
-- doesn't order bytes.
local function bytes_below(a, b)
  if a == b then
    return false
  end
  for i = 1, math.min(#a, #b) do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then
      return x < y
    end
  end
  return #a < #b
end

local TASK_KEY_LEN, EPOCH_KEY_LEN, DIGEST_LEN = 24, 40, 16

-- The `p|<kind>|<producer id>` field of a Producer is 129 bytes, with big-endian numbers. These
-- are the flags, the fence, the held epoch key or zeros, and the last batch's sequence (8), first
-- and last entry ids (the ms in 8 and the seq in 4, zeros for none) and digest. Flag bit 0 is set
-- when the held epoch is present. `docs/redis.md` has the layout.
local PRODUCER_FIELD_LEN = 1 + 2 * EPOCH_KEY_LEN + 8 + 2 * 12 + DIGEST_LEN
local HELD = 1

-- The Producer field as a table, or nil when the Producer has none. A value of another format,
-- which a later version may write, gives false and the refusal.
local function read_producer(meta, field)
  local value = redis.call('HGET', meta, field)
  if not value then
    return nil
  end
  local unknown = refuse('STORAGE_CORRUPT', 'the Producer field has an unknown format')
  if #value ~= PRODUCER_FIELD_LEN then
    return false, unknown
  end
  local flags = string.byte(value, 1)
  if flags ~= 0 and flags ~= HELD then
    return false, unknown
  end
  local at = 2 + 2 * EPOCH_KEY_LEN
  local sequence, first_ms, first_seq, last_ms, last_seq =
    struct.unpack('>I8I8I4I8I4', value, at)
  -- A batch has both entry ids or neither.
  local first_none, last_none = first_ms + first_seq == 0, last_ms + last_seq == 0
  if first_none ~= last_none then
    return false, unknown
  end
  local stored = {
    fence = string.sub(value, 2, 1 + EPOCH_KEY_LEN),
    sequence = sequence,
    first = '',
    last = '',
    digest = string.sub(value, -DIGEST_LEN),
  }
  if flags == HELD then
    stored.held = string.sub(value, 2 + EPOCH_KEY_LEN, 1 + 2 * EPOCH_KEY_LEN)
  end
  if not first_none then
    stored.first = string.format('%.0f-%.0f', first_ms, first_seq)
    stored.last = string.format('%.0f-%.0f', last_ms, last_seq)
  end
  return stored
end

-- The ms and seq of an entry id, or zeros for ''.
local function id_parts(id)
  if id == '' then
    return 0, 0
  end
  local ms, seq = string.match(id, '^(%d+)-(%d+)$')
  return tonumber(ms), tonumber(seq)
end

-- The Producer field of a batch. `held` is nil when no epoch of the Producer has Records, and the
-- ids are '' when the batch wrote no entry.
local function producer_value(fence, held, sequence, first, last, digest)
  local flags, held_key = 0, string.rep('\0', EPOCH_KEY_LEN)
  if held then
    flags, held_key = HELD, held
  end
  local first_ms, first_seq = id_parts(first)
  local last_ms, last_seq = id_parts(last)
  return string.char(flags) .. fence .. held_key ..
    struct.pack('>I8I8I4I8I4', sequence, first_ms, first_seq, last_ms, last_seq) .. digest
end

-- The started event id of a task key, its first 8 bytes.
local function started(key)
  return (struct.unpack('>I8', key))
end

-- Whether the facts of task t show that task key never completed. c is the completed task's key,
-- or nil before the first. `docs/redis.md` has the rule.
local function never_completed(key, t, c, failed_set)
  local s, ts = started(key), started(t)
  local cs = 0
  if c then
    cs = started(c)
  end
  return failed_set[s] == true or (cs < s and s < ts) or (s == ts and bytes_below(key, t))
    or (c ~= nil and s == cs and bytes_below(key, c))
end

-- The started event ids of the failed tasks argument, 8 bytes each, big-endian.
local function failed_ids(failed)
  local ids = {}
  for at = 1, #failed, 8 do
    table.insert(ids, (struct.unpack('>i8', failed, at)))
  end
  return ids
end

-- A task at or before c that the rule doesn't withdraw completed, so `oa` drops it.
local function orphan_tasks(open, t, c, failed)
  local failed_set = {}
  for _, id in ipairs(failed) do
    failed_set[id] = true
  end
  local withdrawn, kept = {}, {}
  for _, key in ipairs(open) do
    if never_completed(key, t, c, failed_set) then
      table.insert(withdrawn, key)
    elseif c == nil or bytes_below(c, key) then
      table.insert(kept, key)
    end
  end
  return withdrawn, kept
end

-- The task key of the Workflow Task with this started event id, with no attempt and no time.
local function started_key(id)
  return struct.pack('>I8', math.max(0, id)) .. string.rep('\0', TASK_KEY_LEN - 8)
end

local ZERO_KEY = started_key(0)

-- The task keys of a field that keeps them back to back, such as `oa|<run>` or `tf|<run>`. A
-- value that ends in a partial key gives false and the refusal.
local function task_keys(value)
  if #value % TASK_KEY_LEN ~= 0 then
    return false, refuse('STORAGE_CORRUPT', 'a field of task keys ends in a partial key')
  end
  local keys = {}
  for at = 1, #value, TASK_KEY_LEN do
    table.insert(keys, string.sub(value, at, at + TASK_KEY_LEN - 1))
  end
  return keys
end

-- Whether the task facts in `tf|<run>` show that task key never completed. The zero key in the
-- completed place works as no completed task, since no task key is below it.
local function fenced_by_facts(facts, key)
  if #facts < 2 then
    return false,
      refuse('STORAGE_CORRUPT', "the run's task facts have fewer than two task keys")
  end
  local failed_set = {}
  for i = 3, #facts do
    failed_set[started(facts[i])] = true
  end
  return never_completed(key, facts[1], facts[2], failed_set)
end
