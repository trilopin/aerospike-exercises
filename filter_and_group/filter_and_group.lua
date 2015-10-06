local function map_generic(rec)
    local names = record.bin_names(rec)
    local ret = map{}
    for i, name in ipairs(names) do
        ret[name] = rec[name]
    end
    return ret
end


function group(stream, bin)
  local function accumulate(currentList, nextElement)
    local key = nextElement[bin]
    if currentList[key] == nil then
      currentList[key] = list()
    end
    list.append(currentList[key], nextElement)
    return currentList
  end

  local function mymerge(a, b)
    return list.merge(a,b)
  end

  local function reducer(this, that)
    return map.merge(this, that, mymerge)
  end

  return stream : map(map_generic) : aggregate(map{}, accumulate) : reduce(reducer)
end