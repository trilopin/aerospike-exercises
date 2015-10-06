local function map_generic(rec)
    local names = record.bin_names(rec)
    local ret = map{}
    for i, name in ipairs(names) do
        ret[name] = rec[name]
    end
    return ret
end


function count(stream, bin)
  local function accumulate(currentList, nextElement)
    local key = nextElement[bin]
    if currentList[key] == nil then
      currentList[key] = 1
    else
      currentList[key] = currentList[key] + 1
    end
    return currentList
  end

  local function mymerge(a, b)
    return a+b
  end

  local function reducer(this, that)
    return map.merge(this, that, mymerge)
  end

  return stream : map(map_generic) : aggregate(map{}, accumulate) : reduce(reducer)
end