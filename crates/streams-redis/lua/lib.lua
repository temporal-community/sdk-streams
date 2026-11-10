-- The prelude of the write scripts. KEYS[1] is the Stream's meta hash and KEYS[2] its log. Each
-- script checks everything that can refuse the call before its first write, since Redis doesn't
-- roll a script back.

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

-- Sets the expiry after this write. An open Stream keeps the safety TTL after its last write, and a
-- closed one keeps the Retention. The meta key expires 30 days later, as the Tombstone. Only a
-- close gives the `closed` key, since a closed Stream takes no other write.
local function refresh(meta, log, closed, now, safety, tombstone)
  local retention = tonumber(redis.call('HGET', meta, 'ret'))
  local exp = now + retention
  if not redis.call('HGET', meta, 'closed') then
    exp = now + safety
  end
  redis.call('HSET', meta, 'exp', exp)
  redis.call('PEXPIREAT', meta, exp + tombstone)
  if closed and redis.call('EXISTS', closed) == 1 then
    redis.call('PEXPIREAT', closed, exp)
  end
  if redis.call('EXISTS', log) == 1 then
    redis.call('PEXPIREAT', log, exp)
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

-- Sets `trimmed` to `newest` when the log is gone, as after an eviction. The log took every Record
-- with it, so a read from an older offset must fail.
local function mark_log_gone(meta, log)
  if redis.call('EXISTS', log) == 1 then
    return
  end
  local newest = redis.call('HGET', meta, 'newest')
  if newest then
    raise_trimmed(meta, newest)
  end
end

-- The id of the script's next entry. A script numbers its entries itself, in one millisecond,
-- since `XADD *` on Redis 7.0 can cross a millisecond inside one script.
local next_ms, next_seq = 0, 0

-- Starts the script's ids after the newest id the log had, which `newest` keeps after the log is
-- gone.
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

local EPOCH_KEY_LEN, DIGEST_LEN = 40, 16

-- The `p|<kind>|<producer id>` field of a Producer is 88 bytes, with big-endian numbers. These are
-- the fence, and the last batch's sequence (8), first and last entry ids (the ms in 8 and the seq
-- in 4, zeros for none) and digest. `docs/redis.md` has the layout.
local PRODUCER_FIELD_LEN = EPOCH_KEY_LEN + 8 + 2 * 12 + DIGEST_LEN

-- The Producer field as a table, or nil when the Producer has none. A value of another length,
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
  local sequence, first_ms, first_seq, last_ms, last_seq =
    struct.unpack('>I8I8I4I8I4', value, EPOCH_KEY_LEN + 1)
  -- A batch has both entry ids or neither.
  local first_none, last_none = first_ms + first_seq == 0, last_ms + last_seq == 0
  if first_none ~= last_none then
    return false, unknown
  end
  local stored = {
    fence = string.sub(value, 1, EPOCH_KEY_LEN),
    sequence = sequence,
    first = '',
    last = '',
    digest = string.sub(value, -DIGEST_LEN),
  }
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

-- The Producer field of a batch. The ids are '' when the batch wrote no entry.
local function producer_value(fence, sequence, first, last, digest)
  local first_ms, first_seq = id_parts(first)
  local last_ms, last_seq = id_parts(last)
  return fence ..
    struct.pack('>I8I8I4I8I4', sequence, first_ms, first_seq, last_ms, last_seq) .. digest
end
