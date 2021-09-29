--the request function that will run at each request
request = function()
    --We define header
    headers = {}
    --We define the path
    path ="/v0/entity?id=key"
    --We return the request object with the current URL path
    return wrk.format("GET",path,headers)
end