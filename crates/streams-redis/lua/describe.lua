-- Returns the newest Record of a Stream.
--
-- KEYS: the meta key.
--
-- Returns the id of the newest Record, `0-0` for none.
local stream, refused = live(KEYS[1], now_ms())
if refused then
  return refused
end
return stream.newest
