-- The start of the scripts that write. A script refuses a call before its first write, since
-- Redis doesn't roll a script back.

-- How long the meta key stays after its Stream expired, so that a call can tell an expired
-- Stream from one that nobody created.
local TOMBSTONE = 30 * 24 * 60 * 60 * 1000

-- Sets when the Stream expires, and the expiry of the meta key and of `keys`.
local function expire_at(meta, keys, at)
  redis.call('HSET', meta, 'exp', at)
  redis.call('PEXPIREAT', meta, at + TOMBSTONE)
  for _, key in ipairs(keys) do
    redis.call('PEXPIREAT', key, at)
  end
end

-- Returns whether the bytes of `a` sort below those of `b`. Lua's `<` orders two strings by the
-- locale of the server.
local function below(a, b)
  for i = 1, math.min(#a, #b) do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then
      return x < y
    end
  end
  return #a < #b
end

-- The length of an attempt key, which starts the value of a Producer.
local ATTEMPT = 40

-- Writes `v` again when a `WAIT` comes after the script, in a call that found its work done. A
-- `WAIT` covers the writes of its own connection up to the last one, so with this write it also
-- covers the earlier write that did the work.
local function touch(meta, wait)
  if wait == '1' then
    redis.call('HSET', meta, 'v', FORMAT)
  end
end
