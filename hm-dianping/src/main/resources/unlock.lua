-- Redis Lua script for unlocking
-- KEYS[1] - lock key
-- ARGV[1] - thread identifier
if ( redis.call('get', KEYS[1]) == ARGV[1]) then
	return redis.call('del', KEYS[1])
end
return 0
