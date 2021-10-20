counter = 0

suffix = "HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD_HIGHlOAD"

request = function()
    path = "/v0/entity?id=key" .. counter
    wrk.method = "GET"
    wrk.body = "value" .. counter .. suffix
    counter = counter + 1
    return wrk.format(nil, path)
end
