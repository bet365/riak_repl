%% Riak EnterpriseDS
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
-module(riak_repl_ring).
-author('Andy Gross <andy@andygross.org>').
-include("riak_repl.hrl").

-export([ensure_config/1,
         initial_config/0,
         get_repl_config/1,
         set_repl_config/2,
         add_site/2,
         add_site_addr/3,
         del_site_addr/3,
         get_site/2,
         del_site/2,
         add_listener/2,
         add_nat_listener/2,
         get_listener/2,
         del_listener/2,
         del_nat_listener/2,
         get_nat_listener/2,
         set_clusterIpAddrs/2,
         get_clusterIpAddrs/2,
         get_clusters/1,
         rt_enable_trans/2,
         rt_disable_trans/2,
         rt_enabled/1,
         rt_start_trans/2,
         rt_stop_trans/2,
         rt_started/1,
         rt_cascades_trans/2,
         rt_cascades/1,
         fs_enable_trans/2,
         fs_disable_trans/2,
         fs_enabled/1,
         set_modes/2,
         get_modes/1,
         pg_enable_trans/2,
         pg_disable_trans/2,
         pg_enabled/1,
         add_nat_map/2,
         del_nat_map/2,
         get_nat_map/1,
         set_filtered_bucket_config/1,
         get_filtered_bucket_config/1,
         get_filtered_bucket_config_for_bucket/2,
         add_filtered_bucket/1,
         remove_cluster_from_bucket_config/1,
         reset_filtered_buckets/1,
         set_bucket_filtering_state/1,
         get_bucket_filtering_state/1,
         remove_filtered_bucket/1
         ]).

-spec(ensure_config/1 :: (ring()) -> ring()).
%% @doc Ensure that Ring has replication config entry in the ring metadata dict.
ensure_config(Ring) ->
    % can't use get_repl_config becuase that's guarenteed to return a config.
    case riak_core_ring:get_meta(?MODULE, Ring) of
        undefined ->
            riak_core_ring:update_meta(?MODULE, initial_config(), Ring);
        _ ->
            Ring
    end.

-spec(get_repl_config/1 :: (ring()) -> riak_repl_dict()|undefined).
%% @doc Get the replication config dictionary from Ring.
get_repl_config(Ring) ->
    case riak_core_ring:get_meta(?MODULE, Ring) of
        {ok, RC} -> RC;
        undefined -> initial_config()
    end.

-spec(set_repl_config/2 :: (ring(), riak_repl_dict()) -> ring()).
%% @doc Set the replication config dictionary in Ring.
set_repl_config(Ring, RC) ->
    riak_core_ring:update_meta(?MODULE, RC, Ring).

-spec(add_site/2 :: (ring(), #repl_site{}) -> ring()).
%% @doc Add a replication site to the Ring.
add_site(Ring, Site=#repl_site{name=Name}) ->
    RC = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    NewSites = case lists:keysearch(Name, 2, Sites) of
        false ->
            [Site|Sites];
        {value, OldSite} ->
            NewSite = OldSite#repl_site{addrs=lists:usort(
                    OldSite#repl_site.addrs ++
                    Site#repl_site.addrs)},
            [NewSite|lists:delete(OldSite, Sites)]
    end,
    riak_core_ring:update_meta(
        ?MODULE,
        dict:store(sites, NewSites, RC),
        Ring).

-spec(del_site/2 :: (ring(), repl_sitename()) -> ring()).
%% @doc Delete a replication site from the Ring.
del_site(Ring, SiteName) ->
    RC  = get_repl_config(Ring),
    Sites = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false ->
            Ring;
        {value, Site} ->
            NewSites = lists:delete(Site, Sites),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(sites, NewSites, RC),
              Ring)
    end.

-spec(get_site/2 :: (ring(), repl_sitename()) -> #repl_site{}|undefined).
%% @doc Get a replication site record from the Ring.
get_site(Ring, SiteName) ->
    RC = get_repl_config(Ring),
    Sites  = dict:fetch(sites, RC),
    case lists:keysearch(SiteName, 2, Sites) of
        false -> undefined;
        {value, ReplSite} -> ReplSite
    end.

-spec(add_site_addr/3 :: (ring(), repl_sitename(), repl_addr()) -> ring()).
%% @doc Add a site address to connect to.
add_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=[Addr|Addrs]});
                true ->
                    Ring
            end
    end.

-spec(del_site_addr/3 :: (ring(), repl_sitename(), repl_addr()) -> ring()).
%% @doc Delete a server address from the site
del_site_addr(Ring, SiteName, {_IP, _Port}=Addr) ->
    case get_site(Ring, SiteName) of
        undefined ->
            Ring;
        #repl_site{addrs=Addrs}=Site ->
            case lists:member(Addr,Addrs) of
                false ->
                    Ring;
                true ->
                    Ring0 = del_site(Ring, SiteName),
                    add_site(Ring0, Site#repl_site{addrs=lists:delete(Addr, Addrs)})
            end
    end.

-spec(add_listener/2 :: (ring(), #repl_listener{}) -> ring()).
%% @doc Add a replication listener host/port to the Ring.
add_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false ->
            NewListeners = [Listener|Listeners],
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, RC),
              Ring);
        true ->
            Ring
    end.

-spec(add_nat_listener/2 :: (ring(), #nat_listener{}) -> ring()).
%% @doc Add a replication NAT listener host/port to the Ring.
add_nat_listener(Ring,NatListener) ->
    RC = get_repl_config(Ring),
    case dict:find(natlisteners,RC) of
        {ok, NatListeners} ->
            case lists:member(NatListener, NatListeners) of
                false ->
                    NewListeners = [NatListener|NatListeners],
                    riak_core_ring:update_meta(
                      ?MODULE,
                      dict:store(natlisteners, NewListeners, RC),
                      Ring);
                true ->
                    Ring
            end;
        error ->
            %% there are no natlisteners entries yet.
            NewListeners = [NatListener],
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(natlisteners, NewListeners, RC),
              Ring)
    end.

-spec(del_listener/2 :: (ring(), #repl_listener{}) -> ring()).
%% @doc Delete a replication listener from the Ring.
del_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    case lists:member(Listener, Listeners) of
        false -> Ring;
        true ->
            NatRing = del_nat_listener(Ring,Listener),
            NewListeners = lists:delete(Listener, Listeners),
            riak_core_ring:update_meta(
              ?MODULE,
              dict:store(listeners, NewListeners, get_repl_config(NatRing)), NatRing)
    end.

-spec(del_nat_listener/2 :: (ring(),#repl_listener{}) -> ring()).
%% @doc Delete a nat_listener from the list of nat_listeners
del_nat_listener(Ring,Listener) ->
    RC  = get_repl_config(Ring),
    case get_nat_listener(Ring, Listener) of
        undefined ->  Ring;
        NatListener ->
            case dict:find(natlisteners, RC) of
                {ok, NatListeners} ->
                    NewNatListeners = lists:delete(NatListener, NatListeners),
                    riak_core_ring:update_meta(
                      ?MODULE,
                      dict:store(natlisteners, NewNatListeners, RC),
                      Ring);
                error -> io:format("Unknown listener~n")
            end
    end.

-spec(get_listener/2 :: (ring(), repl_addr()) -> #repl_listener{}|undefined).
%% @doc Fetch a replication host/port listener record from the Ring.
get_listener(Ring,{_IP,_Port}=ListenAddr) ->
    RC = get_repl_config(Ring),
    case dict:find(listeners, RC) of
        {ok, Listeners}  ->
            case lists:keysearch(ListenAddr, 3, Listeners) of
                false -> undefined;
                {value,Listener} -> Listener
            end;
        error -> undefined
    end.

-spec(get_nat_listener/2 :: (ring(), #repl_listener{}) -> #nat_listener{}|undefined).
%% @doc Fetch a replication nat host/port listener record from the Ring.
get_nat_listener(Ring,Listener) ->
    RC = get_repl_config(Ring),
    case dict:find(natlisteners, RC) of
        {ok, NatListeners} ->
            %% search for a natlistener using only nodename, ip + port,
            %% since nat uses nodename+ip+port+natip+natport as a key
            NatListenerMatches =
                [NatListener ||
                    NatListener <- NatListeners,
                    (NatListener#nat_listener.listen_addr == Listener#repl_listener.listen_addr
                     orelse NatListener#nat_listener.nat_addr == Listener#repl_listener.listen_addr),
                    NatListener#nat_listener.nodename == Listener#repl_listener.nodename],
            %% this will only return the first nat listener that matches
            %% the search criteria
            case NatListenerMatches of
                [NatListener|_] -> NatListener;
                [] -> undefined
            end;
        error -> undefined
    end.

%% set or replace the list of Addrs associated with ClusterName in the ring
set_clusterIpAddrs(Ring, {ClusterName, Addrs}) ->
    RC = get_repl_config(ensure_config(Ring)),
    OldClusters = get_list(clusters, Ring),
    %% replace Cluster in the list of Clusters
    Cluster = {ClusterName, Addrs},
    Clusters = case lists:keymember(ClusterName, 1, OldClusters) of
                   true ->
                       case Addrs of
                           [] ->
                               lists:keydelete(ClusterName, 1, OldClusters);
                           _Addrs ->
                               lists:keyreplace(ClusterName, 1, OldClusters, Cluster)
                       end;
                   _ when Addrs == [] ->
                       %% New cluster has no members, don't add it to the ring
                       OldClusters;
                   _ ->
                       [Cluster | OldClusters]
               end,
    %% replace Clusters in ring
    RC2 = dict:store(clusters, Clusters, RC),
    case RC == RC2 of
        true ->
            %% nothing changed
            {ignore, {not_changed, clustername}};
        false ->
            {new_ring, riak_core_ring:update_meta(
                    ?MODULE,
                    RC2,
                    Ring)}
    end.

%% get list of Addrs associated with ClusterName in the ring
get_clusterIpAddrs(Ring, ClusterName) ->
    Clusters = get_clusters(Ring),
    case lists:keyfind(ClusterName, 1, Clusters) of
        false -> [];
        {_Name,Addrs} -> Addrs
    end.

get_clusters(Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    case dict:find(clusters, RC) of
        {ok, Clusters} ->
            Clusters;
        error ->
            []
    end.


%% Enable proxy_get replication for a given remote
pg_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, pg_enabled, Ring).

%% Disable proxy_get replication for a given remote
pg_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, pg_enabled, Ring).

%% Get list of RT enabled remotes
pg_enabled(Ring) ->
    get_list(pg_enabled, Ring).

%% Enable replication for the remote (queue will start building)
rt_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, rt_enabled, Ring).

%% Disable replication for the remote (queue will be cleaned up)
rt_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, rt_enabled, Ring).

%% Get list of RT enabled remotes
rt_enabled(Ring) ->
    get_list(rt_enabled, Ring).

%% Start replication for the remote - make connection and send
rt_start_trans(Ring, Remote) ->
    add_list_trans(Remote, rt_started, Ring).

%% Stop replication for the remote - break connection and queue
rt_stop_trans(Ring, Remote) ->
    del_list_trans(Remote, rt_started, Ring).

%% Get list of RT started remotes
rt_started(Ring) ->
    get_list(rt_started, Ring).

rt_cascades_trans(Ring, Val) ->
    RC = get_repl_config(ensure_config(Ring)),
    OldVal = case dict:find(realtime_cascades, RC) of
        error ->
            % we want to trigger an update to get the config in the ring
            error;
        {ok, V} ->
            V
    end,
    case Val of
        OldVal ->
            {ignore, {no_change, OldVal}};
        _ when Val =:= always; Val =:= never ->
            {new_ring, riak_core_ring:update_meta(
                              ?MODULE,
                              dict:store(realtime_cascades, Val, RC),
                              Ring)};
        _ ->
            lager:warning("ignoring invalid cascading realtime setting: ~p; using old setting ~p", [Val, OldVal]),
            {ignore, {invalid_option, Val}}
    end.

%% Get status of cascading rt
rt_cascades(Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    % find because the ring may be from a time before cascading rt.
    case dict:find(realtime_cascades, RC) of
        error ->
            never;
        {ok, Val} ->
            Val
    end.

%% Enable replication for the remote (queue will start building)
fs_enable_trans(Ring, Remote) ->
    add_list_trans(Remote, fs_enabled, Ring).

%% Disable replication for the remote (queue will be cleaned up)
fs_disable_trans(Ring, Remote) ->
    del_list_trans(Remote, fs_enabled, Ring).

%% Get list of RT enabled remotes
fs_enabled(Ring) ->
    get_list(fs_enabled, Ring).

initial_config() ->
    dict:from_list(
      [{natlisteners, []},
       {listeners, []},
       {sites, []},
       {realtime_cascades, always},
       {version, ?REPL_VERSION},
       {bucket_filtering_enabled, false}]
      ).

add_list_trans(Item, ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    List = case dict:find(ListName, RC) of
               {ok, List1} ->
                   List1;
               error ->
                   []
           end,
    case lists:member(Item, List) of
        false ->
            NewList = lists:usort([Item|List]),
            {new_ring, riak_core_ring:update_meta(
                         ?MODULE,
                         dict:store(ListName, NewList, RC),
                         Ring)};
        true ->
            {ignore, {already_present, Item}}
    end.

del_list_trans(Item, ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    List = case dict:find(ListName, RC) of
               {ok, List1} ->
                   List1;
               error ->
                   []
           end,
    case lists:member(Item, List) of
        true ->
            NewList = List -- [Item],
            {new_ring, riak_core_ring:update_meta(
                         ?MODULE,
                         dict:store(ListName, NewList, RC),
                         Ring)};
        false ->
            {ignore, {not_present, Item}}
    end.

%% Lookup the list name in the repl config, return empty list if not found
-spec get_list(term(), ring()) -> term().
get_list(ListName, Ring) ->
    RC = get_repl_config(ensure_config(Ring)),
    case dict:find(ListName, RC) of
        {ok, List}  ->
            List;
        error ->
            []
    end.

%% set the "mode" for realtime repl behavior
%% possible values are 
%% v1, v2, v3
set_modes(Ring, NewModes) ->
    % it doesn't make sense to have a mode_repl13_migrating to represent
    % node shutdown repl migration hook since the mode is stored in the
    % repl ring
    ModeCheck = lists:all(fun (Mode) -> 
                            proplists:is_defined(Mode,
                            ?REPL_MODES) end,
                            NewModes)
                          andalso length(NewModes) > 0,
    case ModeCheck of
        true ->
            RC = get_repl_config(Ring),
            % just overwrite whatever was there before
            NewState = riak_core_ring:update_meta(
                ?MODULE,
                dict:store(repl_modes, NewModes, RC),
                Ring),
            %% force the bucket hooks to be reinstalled
            riak_core_ring_manager:force_update(),
            NewState;
        false ->
            lager:warning("Invalid replication modes specified: ~p", [NewModes]),
            Ring
    end.

get_modes(Ring) ->
    RC = get_repl_config(Ring),
    case dict:find(repl_modes, RC) of
        {ok, ReplModes} -> ReplModes;
        error ->
            %% default to mixed modes
            [mode_repl12, mode_repl13]
    end.

add_nat_map(Ring, Mapping) ->
    add_list_trans(Mapping, nat_map, Ring).

del_nat_map(Ring, Mapping) ->
    del_list_trans(Mapping, nat_map, Ring).

get_nat_map(Ring) ->
    get_list(nat_map, Ring).

get_ensured_repl_config(Ring) ->
    get_repl_config(ensure_config(Ring)).

% Wrapper to check whether the ring has changed after setting a new metadata item
-spec check_metadata_has_changed(ring(), riak_repl_dict(), riak_repl_dict()) -> {ignore, {atom(), atom()}} | {new_ring, ring()}.
check_metadata_has_changed(Ring, RC, RC2) ->
    case RC == RC2 of
        true ->
            {ignore, {not_changed, filteredbuckets}};
        false ->
            {new_ring, riak_core_ring:update_meta(?MODULE, RC2, Ring)}
    end.

%% Overwrite current config of filtered buckets
set_filtered_bucket_config({Ring, FilterBucketConfig}) ->
    RC = get_ensured_repl_config(Ring),
    RC2 = dict:store(filteredbuckets, FilterBucketConfig, RC),

    check_metadata_has_changed(Ring, RC, RC2).

% Get the filtered bucket config from the ring
-spec get_filtered_bucket_config(Ring :: ring()) -> [{string(), [binary()]}].
get_filtered_bucket_config(Ring) ->
    get_list(filteredbuckets, Ring).

%% Get the list of buckets to replicate to the cluster 'ClusterName' from the ring config
-spec get_filtered_bucket_config_for_bucket(ring(), string()) -> [binary()] | [].
get_filtered_bucket_config_for_bucket(Ring, BucketName) ->
    case get_filtered_bucket_config(Ring) of
        [] ->
            [];
        FilteredBucketList ->
            case lists:keyfind(BucketName, 1, FilteredBucketList) of
                false ->
                    [];
                {BucketName, FoundConfig} ->
                    FoundConfig
            end
    end.

%% @doc
%% @private
%% Replace the config on the ring metadata for the given bucket, we can be smart and only check if data has changed on
%% replacing the existing data
%% @end
-spec replace_filtered_config_for_bucket(Ring :: ring(), Metadata :: dict(), BucketName :: binary(), NewConfig :: list(list())) -> ring().
replace_filtered_config_for_bucket(Ring, MetaData, BucketName, NewConfig) ->
    case dict:find(filteredbuckets, MetaData) of
        {ok, Config} ->
            case lists:keyfind(BucketName, 1, Config) of
                {BucketName, Config2} ->
                    NewData = lists:keyreplace(BucketName, 1, Config2, NewConfig),
                    RC2 = dict:store(filteredbuckets, NewData, MetaData),
                    check_metadata_has_changed(Ring, MetaData, RC2);
                false ->
                    RC2 = dict:store(filteredbuckets, [{BucketName, NewConfig}], MetaData),
                    {new_ring, riak_core_ring:update_meta(?MODULE, RC2, Ring)}
            end;
        error ->
            RC2 = dict:store(filteredbuckets, [{BucketName, NewConfig}], MetaData),
            % We know the ring has changed as we've added a new key to the metadata, so we'll return it
            {new_ring, riak_core_ring:update_meta(?MODULE, RC2, Ring)}
    end.

%% @doc
%% Add a bucket filtering association to the metadata on the ring. Is stored in the ring metadata as a list of
%% bucket -> [list of clusters]
%% @end
-spec add_filtered_bucket({Ring :: ring(), {ToClusterName :: string(), BucketName :: binary()}}) -> ring().
add_filtered_bucket({Ring, {ToClusterName, BucketName}}) ->
    ClusterName = riak_core_connection:symbolic_clustername(),
    do_add_filtered_bucket(Ring, ClusterName, ToClusterName, BucketName).

-spec do_add_filtered_bucket(Ring :: ring(), FromClusterName :: list(), ToClusterName :: list(), BucketName :: binary()) -> ring().
do_add_filtered_bucket(_Ring, ClusterName, ClusterName, BucketName) ->
    % You can't add a bucket to be replicated to yourself, so ignore if that happens
    lager:warning("tried to add bucket ~s to be filtered to the current cluster", [BucketName]),
    {ignore, {filteredbuckets, same_host}};
do_add_filtered_bucket(Ring, _FromClusterName, ToClusterName, BucketName) ->
    RC = get_ensured_repl_config(Ring),

    %% Get the filtered buckets we currently have
    ClustersToReplicateTo = get_filtered_bucket_config_for_bucket(Ring, BucketName),

    case lists:member(ToClusterName, ClustersToReplicateTo) of
        true ->
            {ignore, {filteredbuckets, bucket_exists}};
        false ->
            NewClusterConfig = [ToClusterName | ClustersToReplicateTo],
            replace_filtered_config_for_bucket(Ring, RC, BucketName, NewClusterConfig)
    end.

% We can enable / disable filtered replication on our side via the ring
-spec set_bucket_filtering_state({Ring :: ring(), Enabled :: boolean()}) -> ring().
set_bucket_filtering_state({Ring, Enabled}) when is_boolean(Enabled) ->
    RC = get_ensured_repl_config(Ring),
    RC2 = dict:store(bucket_filtering_enabled, Enabled, RC),
    check_metadata_has_changed(Ring, RC, RC2).

%% Return a boolean which tells you if bucket filtering is enabled or not
-spec get_bucket_filtering_state(Ring :: ring()) -> [] | proplists:proplist().
get_bucket_filtering_state(Ring) ->
    RC = get_ensured_repl_config(Ring),
    case dict:find(bucket_filtering_enabled, RC) of
        {ok, V} -> V;
        error   -> []
    end.

-spec remove_cluster_from_bucket_config({Ring :: ring(), {Cluster :: list(), Bucket :: binary()}}) -> ring().
remove_cluster_from_bucket_config({Ring, {Cluster, Bucket}}) ->
    RC = get_ensured_repl_config(Ring),

    case get_filtered_bucket_config_for_bucket(Ring, Bucket) of
        [] ->
            {ignore, {filtered_buckets, not_changed}};
        Config ->
            Config2 = lists:delete(Cluster, Config),
            replace_filtered_config_for_bucket(Ring, RC, Bucket, Config2)
    end.

-spec reset_filtered_buckets(Ring :: ring()) -> ring().
reset_filtered_buckets(Ring) ->
    RC = get_ensured_repl_config(Ring),
    RC2 = dict:erase(filteredbuckets, RC),
    check_metadata_has_changed(Ring, RC, RC2).

remove_filtered_bucket({Ring, BucketName}) ->
    RC = get_ensured_repl_config(Ring),
    {ok, BucketConfig} = dict:find(filteredbuckets, RC),
    case lists:keyfind(BucketName, 1, BucketConfig) of
        false ->
            {ignore, {filtered_buckets, not_changed}};
        _ ->
            BucketConfig2 = lists:keydelete(BucketName, 1, BucketConfig),
            RC3 = dict:store(filteredbuckets, BucketConfig2, RC),
            check_metadata_has_changed(Ring, RC, RC3)
    end.


%% unit tests

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

mock_ring() ->
    riak_core_ring:fresh(16, 'test@test').

ensure_config_test() ->
    Ring = ensure_config(mock_ring()),
    ?assertNot(undefined =:= riak_core_ring:get_meta(?MODULE, Ring)),
    Ring.

bucket_filtering_enable_test() ->
    Ring = ensure_config_test(),
    {new_ring, Ring2} = set_bucket_filtering_state({Ring, true}),
    ?assertEqual(true, get_bucket_filtering_state(Ring2)),
    Ring2.

bucket_filtering_can_disable_test() ->
    Ring = bucket_filtering_enable_test(),
    {new_ring, Ring2} = set_bucket_filtering_state({Ring, false}),
    ?assertEqual(false, get_bucket_filtering_state(Ring2)),
    Ring2.

add_get_site_test() ->
    Ring0 = ensure_config_test(),
    Site = #repl_site{name="test"},
    Ring1 = add_site(Ring0, Site),
    Site = get_site(Ring1, "test"),
    Ring1.

add_site_addr_test() ->
    Ring0 = add_get_site_test(),
    Site = get_site(Ring0, "test"),
    ?assertEqual([], Site#repl_site.addrs),
    Ring1 = add_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([{"127.0.0.1", 9010}], Site1#repl_site.addrs),
    Ring1.

del_site_addr_test() ->
    Ring0 = add_site_addr_test(),
    Ring1 = del_site_addr(Ring0, "test", {"127.0.0.1", 9010}),
    Site1 = get_site(Ring1, "test"),
    ?assertEqual([], Site1#repl_site.addrs).

del_site_test() ->
    Ring0 = add_get_site_test(),
    Ring1 = del_site(Ring0, "test"),
    ?assertEqual(undefined, get_site(Ring1, "test")).

add_get_listener_test() ->
    Ring0 = ensure_config_test(),
    Listener = #repl_listener{nodename='test@test',
                              listen_addr={"127.0.0.1", 9010}},
    Ring1 = add_listener(Ring0, Listener),
    Listener = get_listener(Ring1, {"127.0.0.1", 9010}),
    Ring1.

del_listener_test() ->
    Ring0 = add_get_listener_test(),
    Ring1 = del_listener(Ring0, #repl_listener{nodename='test@test',
                                               listen_addr={"127.0.0.1", 9010}}),
    ?assertEqual(undefined, get_listener(Ring1, {"127.0.0.1", 9010})).

add_get_natlistener_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort},
                              nat_addr={NatAddr, NatPort}
                               },
    Ring1 = add_nat_listener(Ring0, NatListener),
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    ?assertNot(undefined == get_nat_listener(Ring1, Listener)),
    Ring1.

%% delete the nat listener using the internal ip address
del_natlistener_test() ->
    Ring0 = add_get_natlistener_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = del_nat_listener(Ring0, Listener),
    %% functions in riak_repl_ring don't automatically add
    %% both regular and nat listeners. So, a regular listener shouldn't appear
    %% in this test
    ?assertEqual(undefined, get_listener(Ring1, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring1, Listener)).

%% delete the nat listener using the public ip address
del_natlistener_publicip_test() ->
    Ring0 = add_get_natlistener_test(),
    NodeName   = "test@test",
    ListenAddr = "10.11.12.13",
    ListenPort = 9011,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    Ring1 = del_nat_listener(Ring0, Listener),
    %% functions in riak_repl_ring don't automatically add
    %% both regular and nat listeners. So, a regular listener shouldn't appear
    %% in this test
    ?assertEqual(undefined, get_listener(Ring1, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring1, Listener)).

get_all_listeners(Ring) ->
    RC = riak_repl_ring:get_repl_config(Ring),
    Listeners = dict:fetch(listeners, RC),
    NatListeners = dict:fetch(natlisteners, RC),
    {Listeners, NatListeners}.

add_del_private_and_publicip_nat_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_listener(Ring2, Listener),
    Ring4 = del_nat_listener(Ring3, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring4),
    ?assertEqual(0,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertEqual(undefined, get_listener(Ring4, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring4, Listener)).

%% similar to test above, however, just delete the nat address
add_del_private_and_publicip_nat2_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_listener(Ring2, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring3),
    ?assertEqual(0,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertEqual(undefined, get_listener(Ring3, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring3, Listener)).

%% similar to test above, however, just delete the nat address
%% This will "downgrade" the listener to a standard listener
add_del_private_and_publicip_nat3_test() ->
    Ring0 = ensure_config_test(),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% you can only get a nat_listener by using a repl_listener
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    %% similar to riak_repl_console:add_nat_listener
    Ring1 = add_listener(Ring0, Listener),
    Ring2 = add_nat_listener(Ring1, NatListener),
    Ring3 = del_nat_listener(Ring2, Listener),
    {Listeners, NatListeners} = get_all_listeners(Ring3),
    ?assertEqual(1,length(Listeners)),
    ?assertEqual(0,length(NatListeners)),
    ?assertNot(undefined == get_listener(Ring3, {ListenAddr, ListenPort})),
    ?assertEqual(undefined, get_nat_listener(Ring3, Listener)).

%% verify that adding a listener, and then a nat listener
%% with the same internal IP "upgrades" the current listener
verify_adding_nat_upgrades_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    NodeName   = "test@test",
    ListenAddr = "127.0.0.1",
    ListenPort = 9010,
    NatAddr    = "10.11.12.13",
    NatPort    = 9011,
    Listener = #repl_listener{nodename=NodeName,
                              listen_addr={ListenAddr, ListenPort}},
    NatListener = #nat_listener{nodename=NodeName,
                                listen_addr={ListenAddr, ListenPort},
                                nat_addr={NatAddr, NatPort}
                               },
    %% add a regular listener
    Ring1 = riak_repl_ring:add_listener(Ring0, Listener),
    %% then add a nat listener. The regular listener now becomes a NAT listener
    Ring2 = riak_repl_ring:add_nat_listener(Ring1, NatListener),
    {Listeners, NatListeners} = get_all_listeners(Ring2),
    ?assertEqual(1,length(Listeners)),
    ?assertEqual(1,length(NatListeners)),
    ?assertNot(undefined == riak_repl_ring:get_listener(Ring2, {ListenAddr, ListenPort})).

%% cascading rt defaults to always
realtime_cascades_defaults_always_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    ?assertEqual(always, riak_repl_ring:rt_cascades(Ring0)).

%% disable rt cascading
realtime_cascades_disable_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    TransRes = riak_repl_ring:rt_cascades_trans(Ring0, never),
    ?assertMatch({new_ring, _Ring}, TransRes),
    {new_ring, Ring1} = TransRes,
    ?assertEqual(never, riak_repl_ring:rt_cascades(Ring1)).

%% disable and re-enable
realtime_cascades_enabled_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    {new_ring, Ring1} = riak_repl_ring:rt_cascades_trans(Ring0, never),
    TransRes = riak_repl_ring:rt_cascades_trans(Ring1, always),
    ?assertMatch({new_ring, _Ring}, TransRes),
    {new_ring, Ring2} = TransRes,
    ?assertEqual(always, riak_repl_ring:rt_cascades(Ring2)).

%% attempt to set to something invalid
realtime_cascades_invalid_set_test() ->
    Ring0 = riak_repl_ring:ensure_config(mock_ring()),
    BadOpt = sometimes,
    ?assertMatch({ignore, {invalid_option, BadOpt}}, riak_repl_ring:rt_cascades_trans(Ring0, BadOpt)).

-endif.
