local function add_bin(total, rec, bin_name)
  return total + (rec[bin_name] or 0)
end

local function merge_sum(left, right)
  return left + right
end

function sum_bin(stream, bin_name)
  return stream : aggregate(0, function(total, rec)
    return add_bin(total, rec, bin_name)
  end) : reduce(merge_sum)
end
