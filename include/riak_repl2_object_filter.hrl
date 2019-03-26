-define(LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(WILDCARD, '*').
-define(DEFAULT_CONFIG(Remote), {Remote, {allow, ['*']}, {block, []}}).
-define(TIMEOUT, 10000).


-define(OBF_CONFIG_KEY, {riak_repl2_object_filter, config}).
-define(OBF_STATUS_KEY, {riak_repl2_object_filter, status}).
-define(OBF_VERSION_KEY, {riak_repl2_object_filter, version}).
