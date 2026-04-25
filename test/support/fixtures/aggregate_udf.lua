local function add_bin(total, rec, bin_name)
  return total + (rec[bin_name] or 0)
end

local function add_age(total, rec)
  return add_bin(total, rec, "age")
end

local function merge_sum(left, right)
  return left + right
end

local function merge_stats(left, right)
  local labels = {}

  for _, label in ipairs(left.labels or {}) do
    table.insert(labels, label)
  end

  for _, label in ipairs(right.labels or {}) do
    table.insert(labels, label)
  end

  return map{
    count = left.count + right.count,
    labels = labels,
    sum = left.sum + right.sum
  }
end

local function collect_stats(acc, rec)
  acc.labels = acc.labels or {}
  acc.sum = acc.sum + (rec["age"] or 0)
  acc.count = acc.count + 1
  table.insert(acc.labels, rec["label"])

  return acc
end

function sum_age(stream, bin_name)
  return stream : aggregate(0, function(total, rec)
    return add_bin(total, rec, bin_name)
  end) : reduce(merge_sum)
end

function age_stats(stream)
  return stream : aggregate(map{sum = 0, count = 0, labels = {}}, collect_stats) : reduce(merge_stats)
end

function sum_summary(stream)
  return stream : aggregate(0, add_age) : reduce(merge_sum) : map(function(total)
    return map{sum = total, labels = {"total", tostring(total)}}
  end)
end

function ages(stream)
  return stream : map(function(rec)
    return rec["age"]
  end)
end

function client_failure(stream)
  return stream : aggregate(0, add_age) : reduce(function(_left, _right)
    error("client reduction failed")
  end)
end

function unsupported_client_helper(stream)
  return stream : groupby(function(value)
    return value
  end)
end
