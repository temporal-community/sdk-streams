-- Reads one page of Records after an entry id.
--
-- KEYS: the meta key, the log.
-- ARGV:
-- 1. The id to read after, `0-0` for the start
-- 2. The maximum number of Records
-- 3. The maximum number of bytes that their entries store
--
-- Returns:
-- 1. `1` for a closed Stream and `0` for an open one
-- 2. The id of the newest Record, `0-0` for none
-- 3. The entries of the page, as `XRANGE` gives them
-- 4. The encoded Producer of the first entry's batch when that batch starts before the page,
--    and nil otherwise
local meta, log = KEYS[1], KEYS[2]
local from, max_records, max_bytes = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])
-- The entries of one `XRANGE`. A page with a byte limit reads at most this many past it.
local CHUNK = 64

local stream, refused = live(meta, now_ms())
if refused then
  return refused
end
if id_below(stream.newest, from) then
  return refuse('OFFSET_TOO_HIGH',
    'The from_offset of the read is past the newest Record of the Stream')
end
local lost = lost_mark(stream, log)
if lost and id_below(from, lost) then
  return refuse('EXPIRED', 'The Stream lost the Records after the from_offset of the read')
end

-- An entry stores less than its encoded Record, so a page that stops here has every Record that
-- fits the caller's limit. The caller makes the exact cut.
local page, bytes, at, more = {}, 0, from, true
while more and #page < max_records do
  local want = math.min(max_records - #page, CHUNK)
  local chunk = redis.call('XRANGE', log, '(' .. at, '+', 'COUNT', want)
  more = #chunk == want
  for _, entry in ipairs(chunk) do
    bytes = bytes + #entry[2][2] + #entry[2][4]
    if bytes > max_bytes and #page > 0 then
      more = false
      break
    end
    page[#page + 1] = entry
    at = entry[1]
  end
end

local producer = false
local first = page[1]
if first and first[2][1] == 'b' then
  local id = string.match(first[1], '^%d+') .. '-' .. first[2][2]
  producer = redis.call('XRANGE', log, id, id)[1][2][2]
end
return {stream.closed and 1 or 0, stream.newest, page, producer}
