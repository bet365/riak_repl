-define(LASTMOD,  <<"X-Riak-Last-Modified">>).
-define(WILDCARD, '*').
-define(DEFAULT_CONFIG(Remote), {Remote, {allow, ['*']}, {block, []}}).
-define(TIMEOUT, 10000).


-define(MERGED_RT_CONFIG, object_filtering_merged_realtime_config).
-define(MERGED_FS_CONFIG, object_filtering_merged_fullsync_config).
-define(REPL_CONFIG, object_filtering_repl_config).
-define(RT_CONFIG, object_filtering_realtime_config).
-define(FS_CONFIG, object_filtering_fullsync_config).

-define(RT_STATUS, object_filtering_realtime_status).
-define(FS_STATUS, object_filtering_fullsync_status).
-define(VERSION, object_filtering_version).
