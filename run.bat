@echo off
set JAR=target/raft-protocol-server-1.0-SNAPSHOT.jar
set CP=-cp %JAR%
set SERVER=org.raft.server.Server
set CLIENT=org.raft.client.Client

start cmd /k java %CP% %SERVER% node1 localhost:5050 node2,localhost:5051 node3,localhost:5052 node4,localhost:5053
start cmd /k java %CP% %SERVER% node2 localhost:5051 node1,localhost:5050 node3,localhost:5052 node4,localhost:5053
start cmd /k java %CP% %SERVER% node3 localhost:5052 node1,localhost:5050 node2,localhost:5051 node4,localhost:5053
start cmd /k java %CP% %SERVER% node4 localhost:5053 node1,localhost:5050 node2,localhost:5051 node3,localhost:5052

start cmd /k java %CP% %CLIENT% localhost:5050