-- Creates a Stream, or finds the one that has the id.
--
-- KEYS: the meta key.
-- ARGV:
-- 1. The request key of the create
-- 2. The Retention, in ms
-- 3. `1` to use an existing Stream
-- 4. `1` when a `WAIT` comes after the script
--
-- Returns 1 when this create made the Stream or is a retry of the create that made it, and 0
-- for a Stream that another create made.
local meta = KEYS[1]
local request, retention, use_existing, wait = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

local now = now_ms()
local stream = load(meta)
if not stream then
  local expires = now + retention
  check_expiry(meta, {}, expires)
  redis.call('HSET', meta, 'v', FORMAT, 'rid', request, 'ret', retention)
  expire_at(meta, {}, expires)
  return 1
end
local refused = unfit(stream, now)
if refused then
  return refused
end
if stream.rid == request then
  touch(meta, wait)
  return 1
end
if use_existing ~= '1' then
  return refuse('ALREADY_EXISTS', 'The Stream already exists')
end
touch(meta, wait)
return 0
