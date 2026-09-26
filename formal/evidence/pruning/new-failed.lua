
                local queue_id = KEYS[1]
                local list_name = KEYS[2]
                local job_data_hash = KEYS[3]
                local dedupe_set_name = KEYS[4]
                local active_hash = KEYS[5]
                local pending_list = KEYS[6]
                local delayed_zset = KEYS[7]

                local other_terminal_list = KEYS[8]
                local results_hash = KEYS[9]

                local pending_cancellations = KEYS[10]

                local max_len = tonumber(ARGV[1])

                local job_ids_to_delete = redis.call('LRANGE', list_name, max_len, -1)
                local actually_deleted = 0

                if #job_ids_to_delete > 0 then
                    -- Trim first so LPOS observes only retained references. Keep
                    -- shared records while either terminal list still names the ID.
                    -- LTRIM 0 -1 retains everything, so zero retention needs DEL.
                    if max_len == 0 then
                        redis.call('DEL', list_name)
                    else
                        redis.call('LTRIM', list_name, 0, max_len - 1)
                    end
                    for _, j_id in ipairs(job_ids_to_delete) do
                        local has_retained_history = redis.call('LPOS', list_name, j_id) ~= false
                            or redis.call('LPOS', other_terminal_list, j_id) ~= false
                        -- CRITICAL FIX: Check if this job_id is currently active/pending/delayed
                        -- This prevents the race where we prune metadata for a job that's currently running
                        -- or about to run (pending). LPOS is O(N) but necessary for correctness when
                        -- job IDs are reused (e.g., eoa_address_chainId pattern).
                        local is_active = redis.call('HEXISTS', active_hash, j_id) == 1
                        -- CRITICAL: Redis nil bulk reply converts to Lua `false`, not `nil`!
                        local lpos_result = redis.call('LPOS', pending_list, j_id)
                        local is_pending = type(lpos_result) == "number"
                        local zscore_result = redis.call('ZSCORE', delayed_zset, j_id)
                        -- ZSCORE is a bulk string when present, false when absent.
                        local is_delayed = zscore_result ~= false
                        
                        -- Only delete if the job is NOT currently in the system
                        if not is_active and not is_pending and not is_delayed and not has_retained_history then
                            local errors_list_name = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':errors'
                            local job_meta_hash = 'twmq:' .. queue_id .. ':job:' .. j_id .. ':meta'

                            redis.call('SREM', dedupe_set_name, j_id)
                            -- No retained/live record remains for this cancellation.
                            redis.call('SREM', pending_cancellations, j_id)
                            redis.call('HDEL', job_data_hash, j_id)
                            redis.call('HDEL', results_hash, j_id)
                            redis.call('DEL', job_meta_hash)
                            redis.call('DEL', errors_list_name)
                            actually_deleted = actually_deleted + 1
                        end
                    end
                end
                return actually_deleted
            