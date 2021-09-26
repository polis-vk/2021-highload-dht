-- upsert entities by consequent keys
---------------------------------------------------------
-- NOTE: each wrk thread has an independent Lua scripting
-- context and thus there will be one counter per thread

counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk.method = "GET"
    counter = counter + 1
    return wrk.format(nil, path)
end

