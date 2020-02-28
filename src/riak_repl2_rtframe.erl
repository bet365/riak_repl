%% API
-module(riak_repl2_rtframe).
-export([encode/2, decode/1]).

%% Protocol 3
-define(MSG_HEARTBEAT,          16#00). %% Heartbeat message
-define(MSG_OBJECTS,            16#10). %% List of objects to write
-define(MSG_ACK,                16#20). %% Ack
-define(MSG_OBJECTS_AND_META,   16#30). %% List of objects and rt meta data

%% Protocol 4
-define(MSG_RECEIVED,           16#40). %% received object
-define(MSG_RETRYING,           16#50). %% retrying
-define(MSG_BT_DROP,           16#60). %% retrying
-define(MSG_NODE_SHUTDOWN,      16#70). %% node shutdown message (so source doesn't rebalance onto the same node)


%% ================================================================================================================== %%
%% Encode Protocol 3
%% ================================================================================================================== %%
%% Build an IOlist suitable for sending over a socket
encode(Type, Payload) ->
    IOL = encode_payload(Type, Payload),
    Size = iolist_size(IOL),
    [<<Size:32/unsigned-big-integer>> | IOL].

encode_payload(objects_and_meta, {Seq, BinObjs, Meta}) when is_binary(BinObjs) ->
    BinsAndMeta = {BinObjs, Meta},
    BinsAndMetaBin = term_to_binary(BinsAndMeta),
    [?MSG_OBJECTS_AND_META, <<Seq:64/unsigned-big-integer>>, BinsAndMetaBin];

encode_payload(objects, {Seq, BinObjs}) when is_binary(BinObjs) ->
    [?MSG_OBJECTS, <<Seq:64/unsigned-big-integer>>, BinObjs];

encode_payload(ack, Seq) ->
    [?MSG_ACK, <<Seq:64/unsigned-big-integer>>];

encode_payload(heartbeat, undefined) ->
    [?MSG_HEARTBEAT];
%% ================================================================================================================== %%
%% Encode Protocol 4
%% ================================================================================================================== %%
encode_payload(recieved, Seq) ->
    [?MSG_RECEIVED, <<Seq:64/unsigned-big-integer>>];

encode_payload(retrying, Seq) ->
    [?MSG_RETRYING, <<Seq:64/unsigned-big-integer>>];

encode_payload(bt_drop, Seq) ->
    [?MSG_BT_DROP, <<Seq:64/unsigned-big-integer>>];

encode_payload(node_shutdown, undefined) ->
    [?MSG_NODE_SHUTDOWN].





%% ================================================================================================================== %%
%% Decode Protocol 3
%% ================================================================================================================== %%
% MsgCode is included in size calc
decode(<<Size:32/unsigned-big-integer, Msg:Size/binary, Rest/binary>>) ->
    <<MsgCode:8/unsigned, Payload/binary>> = Msg,
    {ok, decode_payload(MsgCode, Payload), Rest};

decode(<<Rest/binary>>) ->
    {ok, undefined, Rest}.

decode_payload(?MSG_OBJECTS_AND_META, <<Seq:64/unsigned-big-integer, BinsAndMeta/binary>>) ->
    {Bins, Meta} = binary_to_term(BinsAndMeta),
    {objects_and_meta, {Seq, Bins, Meta}};

decode_payload(?MSG_OBJECTS, <<Seq:64/unsigned-big-integer, BinObjs/binary>>) ->
    {objects, {Seq, BinObjs}};

decode_payload(?MSG_ACK, <<Seq:64/unsigned-big-integer>>) ->
    {ack, Seq};

decode_payload(?MSG_HEARTBEAT, <<>>) ->
    heartbeat;
%% ================================================================================================================== %%
%% Decode Protocol 4
%% ================================================================================================================== %%
decode_payload(?MSG_RECEIVED, <<Seq:64/unsigned-big-integer>>) ->
    {received, Seq};

decode_payload(?MSG_RETRYING, <<Seq:64/unsigned-big-integer>>) ->
    {retrying, Seq};

decode_payload(?MSG_BT_DROP, <<Seq:64/unsigned-big-integer>>) ->
    {bt_drop, Seq};

decode_payload(?MSG_NODE_SHUTDOWN, <<>>) ->
    node_shutdown.
