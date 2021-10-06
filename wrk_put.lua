--the request function that will run at each request
counter =  0
request = function()
    --We define the path
    path ="/v0/entity?id=key" .. counter
    wrk.method = "PUT"
    wrk.body = "value" .. counter
    counter = counter + 1
    --We return the request object with the current URL path
    return wrk.format(nil,path)
end
