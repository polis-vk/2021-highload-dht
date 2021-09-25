-- The uber highload wrk script which can to down any server in the world


counter = 0

request = function()
	path = "/v0/entity?id=key" .. counter
	wrk.method = "PUT"
	wrk.body = "value" .. counter
	counter = counter + 1
	return wrk.format(nil, path)
end
