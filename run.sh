#!/bin/bash

JAR="target/raft-protocol-server-1.0-SNAPSHOT.jar"
CP="-cp $JAR"
SERVER="org.raft.server.Server"
CLIENT="org.raft.client.Client"

# Start Raft servers in separate terminal windows
gnome-terminal -- bash -c "java $CP $SERVER node1 localhost:5050 node2,localhost:5051 node3,localhost:5052 node4,localhost:5053; exec bash"
gnome-terminal -- bash -c "java $CP $SERVER node2 localhost:5051 node1,localhost:5050 node3,localhost:5052 node4,localhost:5053; exec bash"
gnome-terminal -- bash -c "java $CP $SERVER node3 localhost:5052 node1,localhost:5050 node2,localhost:5051 node4,localhost:5053; exec bash"
gnome-terminal -- bash -c "java $CP $SERVER node4 localhost:5053 node1,localhost:5050 node2,localhost:5051 node3,localhost:5052; exec bash"

# Start the client
gnome-terminal -- bash -c "java $CP $CLIENT localhost:5050; exec bash"
