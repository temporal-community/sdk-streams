-- Returns the status and the newest Record of a Stream.
--
-- KEYS: the meta key.
--
-- Returns `1` for a closed Stream and `0` for an open one, and the id of the newest Record,
-- `0-0` for none.
local stream, refused = live(KEYS[1], now_ms())
if refused then
  return refused
end
return {stream.closed and 1 or 0, stream.newest}
