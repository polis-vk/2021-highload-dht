-- upsert entities by consequent keys
---------------------------------------------------------
-- NOTE: each wrk thread has an independent Lua scripting
-- context and thus there will be one counter per thread

counter = 0

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk2.method = "PUT"
    wrk2.body   = "value" .. counter
    counter = counter + 1
    return wrk2.format(nil.path)
end
