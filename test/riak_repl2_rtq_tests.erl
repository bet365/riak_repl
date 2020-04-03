-module(riak_repl2_rtq_tests).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Setup/ Cleanup Functions
%%%===================================================================
meck_setup() ->
    catch(meck:unload(riak_core_metadata)),
    meck:new(riak_core_metadata, [passthrough]),
    meck:expect(riak_core_metadata, get, 2,
        fun(_B, _K) ->
            undefined
        end),
    catch(meck:unload(riak_repl2_reference_rtq)),
    meck:new(riak_repl2_reference_rtq, [passthrough]),
    meck:expect(riak_repl2_reference_rtq, push, 3,
        fun(_,_,_) ->
            ok
        end).

meck_cleanup() ->
    catch(meck:unload(riak_core_metadata)),
    catch(meck:unload(riak_repl2_reference_rtq)).

unset_environment_variables() ->
    application:unset_env(riak_repl, rtq_max_bytes),
    application:unset_env(riak_repl, default_consumer_max_bytes),
    application:unset_env(riak_repl, rtq_concurrency),
    application:unset_env(riak_repl, rtq_overload_threshold),
    application:unset_env(riak_repl, rtq_overload_recover).

%%%===================================================================
%%% RTQ Trim Queue Test
%%%===================================================================
rtq_trim_test() ->

    meck_setup(),
    application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
    application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
    application:set_env(riak_repl, rtq_concurrency, 1),
    {ok, Pid} = riak_repl2_rtq:start(1),

    try
        {0, QTab} = riak_repl2_rtq:register(1, rtq_test),

        %% insert over 20mb in the queue
        MyBin = crypto:rand_bytes(1024*1024),
        [ok = riak_repl2_rtq:push_sync(1, 1, MyBin) || _ <- lists:seq(1, 20)],

        %% we get all 10 bytes back, because in TEST mode the RTQ disregards ETS overhead
        {Size, LastSeq} = get_queue_size(QTab),
        ?assert(Size =< 10*1024*1024),
        ?assert(LastSeq == 20),

        %% the queue is now empty
        _ = [ ok = riak_repl2_rtq:ack_sync(1, rtq_test, X) || X <- lists:seq(10, 20)],
        ?assert(riak_repl2_rtq:is_empty(1))
    after
        meck_cleanup(),
        unset_environment_variables(),
        exit(Pid, kill)
    end.


get_queue_size(QTab) ->
    calculate_size_data_in_queue(QTab, ets:first(QTab), 0).

calculate_size_data_in_queue(QTab, '$end_of_table', Size) ->
    {Size, ets:last(QTab)};
calculate_size_data_in_queue(QTab, Seq, Size) ->
    case ets:lookup(QTab, Seq) of
        [] ->
            calculate_size_data_in_queue(QTab, ets:next(QTab, Seq), Size);
        [{_, _,Bin, _, _}] ->
            calculate_size_data_in_queue(QTab, ets:next(QTab, Seq), Size + byte_size(Bin))
    end.


%%%===================================================================
%%% Status Test
%%%===================================================================
status_test_() ->
    {setup, fun() ->
        meck_setup(),
        application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
        application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
        application:set_env(riak_repl, rtq_concurrency, 1),
        {ok, Pid} = riak_repl2_rtq:start(1),
        {0, _QTab} = riak_repl2_rtq:register(1, rtq_test),
        Pid
    end,
    fun(Pid) ->
        meck_cleanup(),
        unset_environment_variables(),
        exit(Pid, kill)
    end,
    fun(_Pid) -> [

        {"queue size has percentage, and is correct", fun() ->
            MyBin = crypto:rand_bytes(1024 * 1024),
            [riak_repl2_rtq:push_sync(1, 1, MyBin, [{bucket_name, <<"eqc_test">>}]) || _ <- lists:seq(1, 5)],
            Status = riak_repl2_rtq_sup:status(),
            StatusMaxBytes = proplists:get_value(max_bytes, Status),
            StatusBytes = proplists:get_value(bytes, Status),
            StatusPercent = proplists:get_value(percent_bytes_used, Status),
            ExpectedPercent = round( (StatusBytes / StatusMaxBytes) * 100000 ) / 1000,
            ?assertEqual(ExpectedPercent, StatusPercent)
        end}

    ] end}.

%%%===================================================================
%%% Summarize Test
%%%===================================================================
summarize_test_() ->
    {setup,
     fun() ->
         meck_setup(),
         application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
         application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
         application:set_env(riak_repl, rtq_concurrency, 1),
         {ok, Pid} = riak_repl2_rtq:start(1),
         {0, _QTab} = riak_repl2_rtq:register(1, rtq_test),
         Pid
     end,
     fun(Pid) ->
         meck_cleanup(),
         unset_environment_variables(),
         exit(Pid, kill)
     end,
     fun(_Pid) -> [
          {"includes sequence number, object ID, and size",
           fun() ->
               Objects = push_objects(<<"BucketsOfRain">>, [<<"obj1">>, <<"obj2">>]),
               Summarized = riak_repl2_rtq:summarize(1),
               Zipped = lists:zip(Objects, Summarized),
               lists:foreach(
                 fun({Obj, Summary}) ->
                     {Seq, _, _} = Summary,
                     ExpectedSummary = {Seq, riak_object:key(Obj), get_approximate_size(Obj)},
                     ?assertMatch(ExpectedSummary, Summary)
                 end,
                 Zipped)
           end
          }
         ]
     end
}.



evict_test_() ->
    {foreach,
        fun() ->
            meck_setup(),
            application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
            application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
            application:set_env(riak_repl, rtq_concurrency, 1),
            {ok, Pid} = riak_repl2_rtq:start(1),
            {0, _QTab} = riak_repl2_rtq:register(1, rtq_test),
            Pid
        end,
        fun(Pid) ->
            meck_cleanup(),
            unset_environment_variables(),
            exit(Pid, kill)
        end,
     [
      fun(_Pid) ->
          {"evicts object by sequence if present",
           fun() ->
               Objects = push_objects(<<"TwoPeasInABucket">>, [<<"obj1">>, <<"obj2">>]),
               [KeyToEvict, RemainingKey] = [riak_object:key(O) || O <- Objects],
               [{SeqToEvict, KeyToEvict, _}, {RemainingSeq, RemainingKey, _}] = riak_repl2_rtq:summarize(1),
               ok = riak_repl2_rtq:evict(1, SeqToEvict),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize(1)),
               ok = riak_repl2_rtq:evict(1, RemainingSeq + 1),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize(1))
           end
          }
      end,
      fun(_Pid) ->
          {"evicts object by sequence if present and key matches",
           fun() ->
               Objects = push_objects(<<"TwoPeasInABucket">>, [<<"obj1">>, <<"obj2">>]),
               [KeyToEvict, RemainingKey] = [riak_object:key(O) || O <- Objects],
               [{SeqToEvict, KeyToEvict, _}, {RemainingSeq, RemainingKey, _}] = riak_repl2_rtq:summarize(1),
               ?assertMatch({wrong_key, _, _}, riak_repl2_rtq:evict(1, SeqToEvict, RemainingKey)),
               ?assertMatch({not_found, _}, riak_repl2_rtq:evict(1, RemainingSeq + 1, RemainingKey)),
               ?assertEqual(2, length(riak_repl2_rtq:summarize(1))),
               ok = riak_repl2_rtq:evict(1, SeqToEvict, KeyToEvict),
               ?assertMatch([{RemainingSeq, RemainingKey, _}], riak_repl2_rtq:summarize(1))
           end
          }
      end
     ]
    }.


push_objects(Bucket, Keys) -> [push_object(Bucket, O) || O <- Keys].

push_object(Bucket, Key) ->
    RandomData = crypto:rand_bytes(1024 * 1024),
    Obj = riak_object:new(Bucket, Key, RandomData),
    riak_repl2_rtq:push(1, 1, Obj, [{bucket_name, Bucket}]),
    Obj.

object_format() -> riak_core_capability:get({riak_kv, object_format}, v0).
get_approximate_size(O) -> riak_object:approximate_size(object_format(), O).


overload_protection_start_test_() ->
    {setup,
        fun() ->
            meck_setup(),
            application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
            application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
            application:set_env(riak_repl, rtq_concurrency, 1)
        end,
        fun(ok) ->
            meck_cleanup(),
            unset_environment_variables()
        end,
        fun(ok) ->
            [
                {"able to start after a crash without ets errors", fun() ->


                    {ok, Rtq1} = riak_repl2_rtq:start_link(1),
                    unlink(Rtq1),
                    exit(Rtq1, kill),
                    riak_repl_test_util:wait_until_down(Rtq1),
                    Got = riak_repl2_rtq:start_link(1),
                    ?assertMatch({ok, _Pid}, Got),
                    riak_repl2_rtq:stop(1),
                    catch exit(whereis(riak_repl2_rtq), kill),
                    ets:delete(rtq_overload_ets_1),
                    catch(meck:unload(riak_core_metadata))
                end},

                {"start the rtq overload counter process", fun() ->

                    Got1 = riak_repl2_rtq_overload_counter:start_link(1),
                    ?assertMatch({ok, _Pid}, Got1),
                    {ok, Pid1} = Got1,
                    unlink(Pid1),
                    exit(Pid1, kill),
                    riak_repl_test_util:wait_until_down(Pid1),
                    Got2 = riak_repl2_rtq_overload_counter:start_link(1, [{report_interval, 20}]),
                    ?assertMatch({ok, _Pid}, Got2),
                    riak_repl2_rtq_overload_counter:stop(1),
                    catch(meck:unload(riak_core_metadata))
                end}
            ]
        end}.


overload_test_() ->
    {setup,
        fun() ->

            meck_setup(),
            application:set_env(riak_repl, rtq_max_bytes, 10*1024*1024),
            application:unset_env(riak_repl, default_consumer_max_bytes, 10*1024*1024),
            application:set_env(riak_repl, rtq_concurrency, 1),
            application:set_env(riak_repl, rtq_overload_threshold, 5),
            application:set_env(riak_repl, rtq_overload_recover, 1),
            riak_repl_test_util:abstract_stats(),
            {ok, _P1} = riak_repl2_rtq:start(1),
            {ok, _P2} = riak_repl2_rtq_overload_counter:start_link(1, [{report_interval, 1000}]),
            riak_repl2_rtq:register(1, "overload_test")
        end,
        fun(_) ->
            riak_repl2_rtq_overload_counter:stop(1),
            riak_repl2_rtq:stop(1),
            catch exit(whereis(riak_repl2_rtq), kill),
            catch exit(whereis(riak_repl2_rtq_overload_counter), kill),
            meck_cleanup(),
            unset_environment_variables(),
            ets:delete(rtq_overload_ets_1),
            riak_repl_test_util:maybe_unload_mecks([riak_repl_stats]),
            meck:unload(),
            ok
        end,
        fun({QSeq, QTab}) ->
            [
                {"rtq increments sequence number on drop",
                    fun() ->
                        riak_repl2_rtq:push_sync(1, 1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
                        Seq1 = QSeq +1,
                        riak_repl2_rtq:report_drops_sync(1, 5),
                        riak_repl2_rtq:push_sync(1, 1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
                        Seq2 = ets:last(QTab),
                        ?assertEqual(Seq1 + 5 + 1, Seq2)
                    end
                },

                %% TODO: move rtq start into these functions, becuase it is just continuing from the previous test (SEQ problems)
                {"rtq overload reports drops",
                    fun() ->
                        riak_repl2_rtq:push_sync(1, 1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
                        Seq1 = QSeq,
                        [riak_repl2_rtq_overload_counter:drop(1) || _ <- lists:seq(1, 5)],
                        timer:sleep(1200),
                        riak_repl2_rtq:push_sync(1, 1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
                        Seq2 = ets:last(QTab),
                        ?assertEqual(Seq1 + 5 + 1, Seq2)
                    end
                }
%%                {"overload and recovery",
%%                    fun() ->
%%                        % rtq can't process anything else while it's trying to deliver,
%%                        % so we're going to use that to clog up it's queue.
%%                        % Msgq = 0
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        % msg queue = 0 (it's handled)
%%                        block_rtq_pull(),
%%                        % msg queue = 0 (it's handled)
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        % msg queue = 1 (blocked by deliver)
%%                        block_rtq_pull(),
%%                        % msg queue = 2 (blocked by deliver)
%%                        [riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]) || _ <- lists:seq(1,5)],
%%                        % msg queue = 7 (blocked by deliver)
%%                        unblock_rtq_pull(),
%%                        % msq queue = 5 (push handled, blocking deliver handled)
%%                        % that push should have flipped the overload switch
%%                        % meaning these will be dropped
%%                        % these will end up dropped
%%                        [riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]) || _ <- lists:seq(1,5)],
%%                        % msq queue = 7, drops = 5
%%                        unblock_rtq_pull(),
%%                        timer:sleep(1200),
%%                        % msg queue = 0, totol objects dropped = 5
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        Seq1 = pull(5),
%%                        Seq2 = pull(1),
%%                        ?assertEqual(Seq1 + 1 + 5, Seq2),
%%                        Status = riak_repl2_rtq:status(),
%%                        ?assertEqual(5, proplists:get_value(overload_drops, Status))
%%                    end
%%                },
%%                {"rtq does recover on drop report",
%%                    fun() ->
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        block_rtq_pull(),
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        [riak_repl2_rtq ! goober || _ <- lists:seq(1, 10)],
%%                        Seq1 = unblock_rtq_pull(),
%%                        Seq2 = pull(1),
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        timer:sleep(1200),
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        Seq3 = pull(1),
%%                        ?assertEqual(1, Seq1),
%%                        ?assertEqual(2, Seq2),
%%                        ?assertEqual(4, Seq3)
%%                    end
%%                },
%%                {"rtq overload sets rt_dirty to true",
%%                    fun() ->
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        block_rtq_pull(),
%%                        riak_repl2_rtq:push(1, term_to_binary([<<"object">>]), [{bucket_name, <<"eqc_test">>}]),
%%                        [riak_repl2_rtq ! goober || _ <- lists:seq(1, 10)],
%%                        unblock_rtq_pull(),
%%                        History = meck:history(riak_repl_stats),
%%                        ?assertMatch([{_MeckPid, {riak_repl_stats, rt_source_errors, []}, ok}], History)
%%                    end
%%                }
            ]
        end
    }.
%%
%%
%%
%%
%%
%%
%%pull(N) ->
%%    lists:foldl(fun(_Nth, _LastSeq) ->
%%        pull()
%%    end, 0, lists:seq(1, N)).
%%
%%pull() ->
%%    Self = self(),
%%    riak_repl2_rtq:pull("overload_test", fun({Seq, _, _, _}) ->
%%        Self ! {seq, Seq},
%%        ok
%%    end),
%%    get_seq().
%%
%%get_seq() ->
%%    receive {seq, S} -> S end.
%%
%%block_rtq_pull() ->
%%    Self = self(),
%%    riak_repl2_rtq:pull("overload_test", fun({Seq, _, _, _}) ->
%%        receive
%%            continue ->
%%                ok
%%        end,
%%        Self ! {seq, Seq},
%%        ok
%%    end).
%%
%%unblock_rtq_pull() ->
%%    riak_repl2_rtq ! continue,
%%    get_seq().
