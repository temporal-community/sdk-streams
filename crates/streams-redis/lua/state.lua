-- The start of every script. The scripts are the canonical form of the stored format, and
-- `docs/redis.md` describes it.

local FORMAT = 1

local function refuse(reason, message)
  return redis.error_reply('STREAM_ERROR_REASON_' .. reason .. ' ' .. message)
end

local function now_ms()
  local time = redis.call('TIME')
  return time[1] * 1000 + math.floor(time[2] / 1000)
end

-- Returns the `<ms>` and the `<seq>` of an entry id as numbers.
local function id_parts(id)
  local ms, seq = string.match(id, '^(%d+)-(%d+)$')
  return tonumber(ms), tonumber(seq)
end

local function id_below(a, b)
  local a_ms, a_seq = id_parts(a)
  local b_ms, b_seq = id_parts(b)
  return a_ms < b_ms or (a_ms == b_ms and a_seq < b_seq)
end

-- Returns the fields of a Stream, or nil when nobody created it. `newest` is `0-0`, which is
-- never the id of an entry, while the Stream has no Record.
local function load(meta)
  local fields =
    redis.call('HMGET', meta, 'v', 'rid', 'ret', 'idle', 'exp', 'closed', 'newest', 'lost')
  if not fields[1] then
    return nil
  end
  return {
    v = fields[1],
    rid = fields[2],
    ret = fields[3],
    idle = fields[4],
    exp = fields[5],
    closed = fields[6],
    newest = fields[7] or '0-0',
    lost = fields[8],
  }
end

-- Returns the refusal of a Stream that takes no call, or nil.
local function unfit(stream, now)
  if tonumber(stream.v) > FORMAT then
    return refuse('FORMAT_TOO_NEW',
      'The Stream has format ' .. stream.v .. ', and this Store knows format ' .. FORMAT)
  end
  if stream.exp and tonumber(stream.exp) <= now then
    return refuse('EXPIRED', 'The Stream expired')
  end
  return nil
end

-- Returns the fields of a Stream that takes calls, or nil and the refusal.
local function live(meta, now)
  local stream = load(meta)
  if not stream then
    return nil, refuse('NOT_FOUND', 'The Stream was never created')
  end
  return stream, unfit(stream, now)
end

-- Returns the id up to which the log lost its Records, or nil. The Store deletes no entry, so
-- the log has lost every Record when the newest one is missing.
local function lost_mark(stream, log)
  if stream.newest ~= '0-0' and #redis.call('XRANGE', log, stream.newest, stream.newest) == 0 then
    return stream.newest
  end
  return stream.lost
end
