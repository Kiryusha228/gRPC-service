box.cfg {
    listen = tonumber(os.getenv("PORT"))
}

local user_name = os.getenv("CRUD_USER") or "crud_user"
local user_pass = os.getenv("CRUD_PASS") or "password"

if not box.schema.user.exists(user_name) then
    box.schema.user.create(user_name, { password = user_pass })
end

local kvspace = box.schema.space.create('KV', {
    if_not_exists = true,
    format = {
            {name = 'key', type = 'string'},
            {name = 'value', type = 'varbinary', is_nullable = true}
        }
})

kvspace:create_index('primary', {
    type = 'tree',
    parts = {
        {field = 'key', type = 'string'}
    },
    if_not_exists = true
})

box.schema.func.create('kv_put', {
    body = [[
        function(key, value)
            return box.space.KV:replace({key, value})
        end
    ]],
    language = 'Lua'
})

box.schema.func.create('kv_get', {
    body = [[
        function(key)
            local tuple = box.space.KV:get({key})
            return tuple and tuple[2] or nil
        end
    ]],
    language = 'Lua'
})

box.schema.func.create('kv_delete', {
    body = [[
        function(key)
            return box.space.KV:delete({key})
        end
    ]],
    language = 'Lua'
})

box.schema.func.create('kv_range', {
    body = [[
        function(key_since, key_to, limit)
            local result = {}
            local index = box.space.KV.index.primary

            local count = 0

            for _, tuple in index:pairs({key_since}, { iterator = 'GE' }) do
                local key = tuple[1]

                if key > key_to or count >= limit then
                    break
                end

                table.insert(result, {key, tuple[2]})
                count = count + 1
            end

            return result
        end
    ]],
    language = 'Lua',
    is_deterministic = true
})

box.schema.func.create('kv_count', {
    body = [[
        function()
            return box.space.KV:count()
        end
    ]],
    language = 'Lua'
})

box.schema.user.grant(user_name, 'read,write', 'space', 'KV')

for _, fname in ipairs({'kv_put','kv_get','kv_delete','kv_range','kv_count'}) do
    box.schema.user.grant(user_name, 'execute', 'function', fname)
end
