#!/bin/bash
# from https://github.com/wurstmeister/kafka-docker/issues/167#issuecomment-309823061

r=`$KAFKA_HOME/bin/zookeeper-shell.sh zk-headless:2181 <<< "ls /brokers/ids" | tail -1 | jq '.[]'`
ids=( $r )
function contains () {
  local n=$#
  local value=${!n}
  for ((i=1;i < $#;i++)) {
    if [ "${!i}" == "${value}" ]; then
      echo "y"
      return 0
    fi
  }
  echo "n"
  return 1
}

x=`cat $KAFKA_HOME/config/server.properties | awk 'BEGIN{FS="="}/^broker.id=/{print $2}'`
if [ $(contains "${ids[@]}" "$x") == "y" ]; then echo "ok"; exit 0; else echo "failed"; exit 1; fi`
