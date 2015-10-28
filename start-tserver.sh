#! /bin/bash

/Users/dan/src/cloudera/kudu/build/latest/kudu-tserver \
--fs_wal_dir=/tmp/test-tserver/wal \
--fs_data_dirs=/tmp/test-tserver/data \
--rpc_bind_addresses=127.28.68.0:0 \
--local_ip_for_outbound_sockets=127.28.68.0 \
--webserver_interface=127.28.68.0 \
--webserver_port=0 \
--tserver_master_addrs=127.0.0.1:55329 \
--metrics_log_interval_ms=1000 \
--log_dir=/tmp/test-tserver/log \
--memory_limit_hard_bytes=67108864 \
--memory_limit_soft_percentage=0 \
--server_dump_info_path=/tmp/test-tserver/dump \
--server_dump_info_format=pb \
--rpc_server_allow_ephemeral_ports \
--logtostderr \
--logbuflevel=-1
