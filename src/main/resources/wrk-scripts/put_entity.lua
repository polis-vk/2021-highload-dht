counter = 0

request = function()
   path = "/v0/entity?id=" .. counter
   wrk.method = "PUT"
   wrk.body = string.rep("s", 4096)
   counter = counter + 1
   return wrk.format(nil, path)
end