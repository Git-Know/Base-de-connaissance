#!/bin/bash
echo $ZOO_MY_ID > /data/myid
exec /docker-entrypoint.sh zkServer.sh start-foreground
