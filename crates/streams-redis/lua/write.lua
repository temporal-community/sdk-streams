-- Redis does not roll back script errors, so reject known failures before writing.

-- How long the meta key stays after its Stream expired, so that a call can tell an expired
-- Stream from one that nobody created.
local TOMBSTONE = 30 * 24 * 60 * 60 * 1000

-- Redis authorizes each command as it runs. Check all target keys before changing any
-- of them, including deployments that grant permissions with separate ACL selectors.
local function require_write(cmd, ...)
  if not redis.acl_check_cmd(cmd, ...) then
    error('ACL failure in script: the user lacks permission for command ' .. cmd)
  end
end

local function check_expiry(meta, keys, at)
  require_write('HSET', meta, 'exp', at)
  require_write('PEXPIREAT', meta, at + TOMBSTONE)
  for _, key in ipairs(keys) do
    require_write('PEXPIREAT', key, at)
  end
end

local function expire_at(meta, keys, at)
  redis.call('HSET', meta, 'exp', at)
  redis.call('PEXPIREAT', meta, at + TOMBSTONE)
  for _, key in ipairs(keys) do
    redis.call('PEXPIREAT', key, at)
  end
end

-- Lua string order depends on the server locale. Attempt keys need unsigned byte order.
local function below(a, b)
  for i = 1, math.min(#a, #b) do
    local x, y = string.byte(a, i), string.byte(b, i)
    if x ~= y then
      return x < y
    end
  end
  return #a < #b
end

local ATTEMPT = 28

-- `WAIT` covers only writes on this connection. A new write also covers the original batch.
local function touch(meta, wait)
  if wait == '1' then
    redis.call('HSET', meta, 'v', FORMAT)
  end
end
