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
-- Producer (1) closes a Workflow-owned Stream, with a ref of the run chain in `oc`. A Local
-- Activity (2) can't, since it writes before its task commits. A client (4) closes the others.
local function check_owner(meta, kind, chain)
  if redis.call('HGET', meta, 'own') == 'wf' then
    if kind ~= '1' then
      return refuse('NOT_OWNER', 'only the close Activity of the owner run chain closes a ' ..
        'Workflow-owned Stream')
    end
    local owner_chain = redis.call('HGET', meta, 'oc')
    if owner_chain and chain ~= owner_chain then
      return refuse('NOT_OWNER', 'the ref names another run chain than the owner')
    end
  elseif kind ~= '4' then
    return refuse('NOT_OWNER', 'only a client closes a Stream that a client created')
  end
  return nil
end

-- Returns nil when a create names the Stream's owner, or else the refusal. The owner is part of
-- the Stream's identity, so a create can't change it.
local function check_identity(meta, owner, chain)
  local stored = redis.call('HGET', meta, 'oc') or ''
  if redis.call('HGET', meta, 'own') ~= owner or stored ~= chain then
    return refuse('NOT_OWNER', 'the Stream has another owner than the create names')
  end
  return nil
end

-- Sets the expiry after this write. It's the safety TTL for an open Workflow-owned Stream, or the
-- Retention otherwise. The meta key expires 30 days later, as the Tombstone.
local function refresh(meta, log, producers, now, safety, tombstone)
  local retention = tonumber(redis.call('HGET', meta, 'ret'))
  local exp = now + retention
  if redis.call('HGET', meta, 'own') == 'wf' and not redis.call('HGET', meta, 'closed') then
    exp = now + safety
  end
  redis.call('HSET', meta, 'exp', exp)
  redis.call('PEXPIREAT', meta, exp + tombstone)
  if redis.call('EXISTS', producers) == 1 then
    redis.call('PEXPIREAT', producers, exp)
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
-- back must not move the watermark back.
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
    -- The expired log took every Record, so a read from an older offset must fail.
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

-- Removes the fence and dedupe fields of the Producers whose last append is older than the
-- Retention. Their Records are older too, so they hold none.
local function prune(meta, producers, now)
  local min = now - tonumber(redis.call('HGET', meta, 'ret'))
  local old = redis.call('ZRANGEBYSCORE', producers, '-inf', '(' .. min, 'LIMIT', 0, PRUNE_LIMIT)
  for _, producer in ipairs(old) do
    redis.call('HDEL', meta, 'e|' .. producer, 'd|' .. producer, 'h|' .. producer)
  end
  if #old > 0 then
    redis.call('ZREM', producers, unpack(old))
  end
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
    return refuse('STORAGE', 'the entry ids of this millisecond would not fit an offset')
  end
  return nil
end

-- Adds an entry with the script's next id. A script calls `check_ids_fit` before its first one.
local function add(log, fields)
  local id = string.format('%d-%d', next_ms, next_seq)
  next_seq = next_seq + 1
  return redis.call('XADD', log, id, unpack(fields))
end
