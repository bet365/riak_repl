-module(riak_repl2_rtsource_conn_data_mgr_tests).
-author("nordine saadouni").

-include_lib("eunit/include/eunit.hrl").

-define(SOURCE_3, [node_c, node_b, node()]).
-define(SOURCE_5, [node_e, node_d, node_c, node_b, node()]).
-define(SOURCE_8, [node_h, node_g, node_f, node_e, node_d, node_c, node_b, node()]).

data_manager_test_() ->
  {spawn,
    [
      {setup,
        fun() ->
          Apps = riak_repl_test_util:maybe_start_lager(),
          _MockRing = ets:new(mock_ring_test, [named_table, public]),

          catch(meck:unload(riak_core_capability)),
          meck:new(riak_core_capability, [passthrough]),
          meck:expect(riak_core_capability, get, 1, fun(_) -> v1 end),
          meck:expect(riak_core_capability, get, 2, fun(_, _) -> v1 end),

          catch(meck:unload(riak_repl2_leader)),
          meck:new(riak_repl2_leader, [passthrough]),
          meck:expect(riak_repl2_leader, leader_node, 0, fun() -> node() end),
          meck:expect(riak_repl2_leader, re_notify, 0, fun() -> ok end),

          riak_core_node_watcher_events_start(),

          %% ring_trans
          riak_core_ring_manager_start(),
          %% nodes
          riak_core_node_watcher_start(),
          %% get_active_nodes, get_realtime_connection_data
          riak_repl_ring_start(),
          {ok, _Pid} = riak_repl2_rtsource_conn_data_mgr:start_link(),
          riak_repl2_rtsource_conn_sup_start(),
          riak_repl2_rtsource_conn_data_mgr:set_leader(node(), pid),
          Apps
        end,
        fun(StartedApps) ->
          process_flag(trap_exit, true),
          catch(exit(whereis(riak_repl2_rtsource_conn_data_mgr), kill)),
          catch(meck:unload(riak_core_node_watcher)),
          catch(meck:unload(riak_core_ring_manager)),
          catch(meck:unload(riak_repl_ring)),
          catch(meck:unload(riak_repl2_rtsource_conn_sup)),
          catch(meck:unload(riak_core_capability)),
          catch(meck:unload(riak_core_node_watcher_events)),
          ets:delete(mock_ring_test),
          timer:sleep(200),
          process_flag(trap_exit, false),
          riak_repl_test_util:stop_apps(StartedApps),
          ok
        end,

        fun(_) ->
          [
            {"Initialization",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:node_watcher_update([riak_kv]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                [{active_nodes, AN}] = ets:lookup(mock_ring_test, active_nodes),
                ?assertMatch(RTC, dict:new()),
                ?assertMatch(AN, ?SOURCE_5)
              end
            },

            {"Test 1 - Write 1",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_a, {ip1, port1}, true),
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_b, {ip2, port2}, true),
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_c, {ip3, port3}, true),
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_d, {ip4, port4}, true),
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_e, {ip5, port5}, true),
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_f, {ip6, port6}, true),
                timer:sleep(100),

                riak_repl2_rtsource_conn_data_mgr:node_watcher_update([riak_kv]),


                NodeDict = dict:from_list([
                  {node_a, [{{ip1,port1},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_e, [{{ip5,port5},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict}
                ]),

                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 2 - Write 2",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote", node_a, [{{ip1,port2},false}, {{ip10, port10},true}]),
                timer:sleep(100),
                NodeDict = dict:from_list([
                  {node_a, [{{ip1,port2},false}, {{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_e, [{{ip5,port5},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict}
                ]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 3 - Write 3",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:write(realtime_connections, "remote2", node_2, [{{ip1, port1},true}]),
                timer:sleep(200),
                NodeDict = dict:from_list([
                  {node_a, [{{ip1,port2},false}, {{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_e, [{{ip5,port5},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                NodeDict2 = dict:from_list([
                  {node_2, [{{ip1, port1}, true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict},
                  {"remote2", NodeDict2}
                ]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 4 - Delete 1",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, "remote", node_a, {ip1, port2}, false),
                timer:sleep(100),
                NodeDict = dict:from_list([
                  {node_a, [{{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_e, [{{ip5,port5},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                NodeDict2 = dict:from_list([
                  {node_2, [{{ip1, port1}, true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict},
                  {"remote2", NodeDict2}
                ]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 5 - Delete 2",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, "remote", node_e),
                timer:sleep(100),
                NodeDict = dict:from_list([
                  {node_a, [{{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                NodeDict2 = dict:from_list([
                  {node_2, [{{ip1, port1}, true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict},
                  {"remote2", NodeDict2}
                ]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 6 - Read 1",
              fun() ->
                Remote = riak_repl2_rtsource_conn_data_mgr:read(realtime_connections, "remote"),
                Remote2 = riak_repl2_rtsource_conn_data_mgr:read(realtime_connections, "remote2"),
                NodeDict = dict:from_list([
                  {node_a, [{{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                NodeDict2 = dict:from_list([
                  {node_2, [{{ip1, port1}, true}]}
                ]),
                ?assertMatch(Remote, NodeDict),
                ?assertMatch(Remote2, NodeDict2)

              end
            },

            {"Test 7 - Delete 3",
              fun() ->
                riak_repl2_rtsource_conn_data_mgr:delete(realtime_connections, "remote2"),
                timer:sleep(100),
                NodeDict = dict:from_list([
                  {node_a, [{{ip10, port10},true}]},
                  {node_b, [{{ip2,port2},true}]},
                  {node_c, [{{ip3,port3},true}]},
                  {node_d, [{{ip4,port4},true}]},
                  {node_f, [{{ip6,port6},true}]}
                ]),
                ExpectedRealtime = dict:from_list([
                  {"remote", NodeDict}
                ]),
                [{realtime_connections, RTC}] = ets:lookup(mock_ring_test, realtime_connections),
                ?assertMatch(ExpectedRealtime, RTC)
              end
            },

            {"Test 8 - Read 2",
              fun() ->
                NodeA = riak_repl2_rtsource_conn_data_mgr:read(realtime_connections, "remote", node_a),
                ?assertMatch(NodeA, [{{ip10, port10},true}])
              end
            },

            {"Test 9 - Read 1 (Polling)",
              fun() ->
                dynamic_node_watcher(?SOURCE_3),
                riak_repl2_rtsource_conn_data_mgr:node_watcher_update([riak_kv]),
                timer:sleep(100),
                ActiveNodes = riak_repl2_rtsource_conn_data_mgr:read(active_nodes),
                [{active_nodes, AN}] = ets:lookup(mock_ring_test, active_nodes),
                ?assertMatch(ActiveNodes, ?SOURCE_3),
                ?assertMatch(AN, ?SOURCE_3)
              end
            },

            {"Test 10 - Read 2 (Polling)",
              fun() ->
                dynamic_node_watcher(?SOURCE_8),
                riak_repl2_rtsource_conn_data_mgr:node_watcher_update([riak_kv]),
                timer:sleep(100),
                ActiveNodes = riak_repl2_rtsource_conn_data_mgr:read(active_nodes),
                [{active_nodes, AN}] = ets:lookup(mock_ring_test, active_nodes),
                ?assertMatch(ActiveNodes, ?SOURCE_8),
                ?assertMatch(AN, ?SOURCE_8)
              end
            }



          ]
        end
      }
      ]
  }.


%%------------------------
%% Start up functions
%%------------------------
riak_core_ring_manager_start() ->
  Fun1 = fun riak_repl_ring:overwrite_realtime_connection_data/2,
  Fun2 = fun riak_repl_ring:overwrite_active_nodes/2,
  Fun3 = fun riak_repl_ring:overwrite_active_nodes_and_realtime_connection_data/2,
  catch(meck:unload(riak_core_ring_manager)),
  meck:new(riak_core_ring_manager, [passthrough]),
  meck:expect(riak_core_ring_manager, ring_trans,
    fun(X,Y) ->
      case {X, Y} of
        {Fun1, NewConnections} ->
          ets:insert(mock_ring_test, {realtime_connections, NewConnections}),
          ok;
        {Fun2, NewActiveNodes} ->
          ets:insert(mock_ring_test, {active_nodes,NewActiveNodes}),
          ok;
        {Fun3, {NewConnections, NewActiveNodes}} ->
          ets:insert(mock_ring_test, {realtime_connections, NewConnections}),
          ets:insert(mock_ring_test, {active_nodes,NewActiveNodes}),
          ok
      end
    end
  ).

riak_core_node_watcher_events_start() ->
  catch(meck:unload(riak_core_node_watcher_events)),
  meck:new(riak_core_node_watcher_events, [passthrough]),
  meck:expect(riak_core_node_watcher_events, add_sup_callback,
    fun(_Fn) ->
      ok
    end).

riak_core_node_watcher_start() ->
  catch(meck:unload(riak_core_node_watcher)),
  meck:new(riak_core_node_watcher, [passthrough]),
  dynamic_node_watcher(?SOURCE_5).

riak_repl_ring_start() ->
  catch(meck:unload(riak_repl_ring)),
  meck:new(riak_repl_ring, [passthrough]),
  meck:expect(riak_repl_ring, get_active_nodes,
    fun() ->
      [{active_nodes, ActiveNodes}] = ets:lookup(mock_ring_test, active_nodes),
      ActiveNodes
    end
  ),
  meck:expect(riak_repl_ring, get_realtime_connection_data,
    fun() ->
      [{realtime_connections, RealtimeConnections}] = ets:lookup(mock_ring_test, realtime_connections),
      RealtimeConnections
    end
  ).

riak_repl2_rtsource_conn_sup_start() ->
  catch(meck:unload(riak_repl2_rtsource_conn_sup)),
  meck:new(riak_repl2_rtsource_conn_sup, [passthrough]),
  meck:expect(riak_repl2_rtsource_conn_sup, enabled, fun() -> [] end).


% ----------------------------------------- %
%         Dynamic Meck Expects              %
% ----------------------------------------- %
dynamic_node_watcher(SourceNodes) ->
  meck:expect(riak_core_node_watcher, nodes,
    fun(_) ->
      SourceNodes
    end
  ).