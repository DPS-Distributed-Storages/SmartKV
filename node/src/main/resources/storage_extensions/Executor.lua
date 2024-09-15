
local executor = {}

function executor.invokeMethod(function_name, object, ...)
    status, result = pcall(object[function_name], object, ...)
    if status == true then
        return JSON:encode(result)
    else
        error("The invoked method " .. function_name .. " with parameters [" .. table.concat({...}, ", ") .. "] could not be invoked! " .. result)
    end
end

function executor.construct(class, ...)
    return class:new(nil, ...)
end

return executor