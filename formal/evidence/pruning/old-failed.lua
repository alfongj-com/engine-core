
                local queue_id = KEYS[1]
                local list_name = KEYS[2]
                local job_data_hash = KEYS[3]
                local dedupe_set_name = KEYS[4]
                local active_hash = KEYS[5]
                local pending_list = KEYS[6]
                local delayed_zset = KEYS[7]

                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)
                local actually_deleted = 0

                if #job_ids_to_delete > 0 then
                    for _, j_id in ipairs(job_ids_to_delete) do
                        -- CRITICAL FIX: Check if this job_id is currently active/pending/delayed
                        -- This prevents the race where we prune metadata for a job that's currently running
                        -- or about to run (pending). LPOS is O(N) but necessary for correctness when
                        -- job IDs are reused (e.g., eoa_address_chainId pattern).
                        local is_active = redis.call('HEXISTS', active_hash, j_id) == 1
                        -- CRITICAL: Redis nil bulk reply converts to Lua `false`, not `nil`!
                        local lpos_result = redis.call('LPOS', pending_list, j_id)
                        local is_pending = type(lpos_result) == "number"
                        local zscore_result = redis.call('ZSCORE', delayed_zset, j_id)
                        local is_delayed = type(zscore_result) == "number"
                        
                        -- Only delete if the job is NOT currently in the system
                        if not is_active and not is_pending and not is_delayed then
                            local errors_list_name = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':errors'
                            local job_meta_hash = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':meta'

                            redis.call('SREM', dedupe_set_name, j_id)
                            redis.call('HDEL', job_data_hash, j_id)
                            redis.call('DEL', job_meta_hash)
                            redis.call('DEL', errors_list_name)
                            actually_deleted = actually_deleted + 1
                        end
                    end
                    redis.call('LTRIM', list_name, 0, max_len - 1)
                end
                return actually_deleted
            