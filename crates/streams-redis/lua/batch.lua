-- The prelude of the read scripts. A read finds the Producer of a page's oldest entry in the same
-- script, so the log can't expire between the page and the lookup.

local function field(fields, name)
  for i = 1, #fields, 2 do
    if fields[i] == name then
      return fields[i + 1]
    end
  end
end

-- Returns the encoded Producer of the batch of `entry`, when that batch starts before `entry`, or
-- else ''. A batch's later entries keep the seq of its first entry, which keeps the Producer.
local function batch_producer(log, entry)
  local start = entry and field(entry[2], 'b')
  if not start then
    return ''
  end
  local id = string.match(entry[1], '^%d+-') .. start
  local first = redis.call('XRANGE', log, id, id)[1]
  return first and field(first[2], 'p') or ''
end
