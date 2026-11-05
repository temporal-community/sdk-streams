-- Creates the Stream's meta key, or finds it. On a Stream that exists, it refuses one that is
-- expired, has another owner or is closed, in that order. Then a retry of the create that made the
-- Stream gets `created` again. Any other create fails with `ALREADY_EXISTS`, unless it uses the
-- existing Stream.
--
-- ARGV: format version, Retention ms, owner (`wf` or `client`), safety TTL ms, Tombstone ms,
-- encoded withdrawn run, Superseded facts, the owner's run chain as
-- `<workflow id>:<first run id>` with each part encoded (empty for a client), the request id
-- (empty for none), `1` when the create uses an existing Stream, `1` when a `WAIT` runs after it.
--
-- Returns {outcome, stored Retention ms, '1' when flagged}. The outcome is `created` or `exists`.
-- A Stream that exists keeps its own Retention.
local meta, log, producers = KEYS[1], KEYS[2], KEYS[3]
local version, retention, owner = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local safety, tombstone = tonumber(ARGV[4]), tonumber(ARGV[5])
local withdrawn_run, superseded = ARGV[6], ARGV[7]
local chain, request_id, use_existing, wait = ARGV[8], ARGV[9], ARGV[10], ARGV[11]

local now = now_ms()
local stored = redis.call('HGET', meta, 'ret')
if not stored then
  redis.call('HSET', meta, 'v', version, 'ret', retention, 'own', owner)
  if chain ~= '' then
    redis.call('HSET', meta, 'oc', chain)
  end
  if request_id ~= '' then
    redis.call('HSET', meta, 'rid', request_id)
  end
  withdraw_run(meta, withdrawn_run)
  refresh(meta, log, producers, now, safety, tombstone)
  return {'created', retention, ''}
end
local refused = check_live(meta, version, now) or check_identity(meta, owner, chain)
if refused then
  return refused
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
-- A retry of the create that made the Stream gets its answer again, after a `WAIT` that covers
-- the first create's write.
if request_id ~= '' and redis.call('HGET', meta, 'rid') == request_id then
  touch(meta, wait)
  return {'created', stored, ''}
end
if use_existing ~= '1' then
  return refuse('ALREADY_EXISTS', 'the Stream exists')
end
-- A retry run's create may write its Superseded item, so its id must fit an offset too.
if withdrawn_run ~= '' then
  refused = check_ids_fit(meta, log, now, 1)
  if refused then
    return refused
  end
end
local flag = ''
if withdraw_run(meta, withdrawn_run) then
  trim(meta, log, now)
  local id = add(log, {'s', superseded})
  redis.call('HSET', meta, 'newest', id)
  refresh(meta, log, producers, now, safety, tombstone)
  flag = '1'
end
touch(meta, wait)
return {'exists', stored, flag}
