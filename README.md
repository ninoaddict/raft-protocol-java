# Raft Protocol Implementation for Key-Value Store

This project implements the Raft consensus protocol for a distributed key-value store system based on the [original Raft paper](https://raft.github.io/raft.pdf). The implementation uses Java and gRPC for network communication between nodes.

## Features

- Complete implementation of Raft consensus protocol
  - Leader Election
  - Log Replication
  - Safety properties
- Distributed key-value store with the following operations:
  - GET: Retrieve a value by key
  - SET: Store a key-value pair
  - DEL: Delete a key-value pair
  - STRLEN: Get the length of a stored value
  - APPEND: Append a value to an existing key
- Dynamic cluster membership changes
- Built with gRPC for efficient network communication
- Containerized deployment using Docker

## Prerequisites

- Java 21 or higher
- Maven
- Docker and Docker Compose (for containerized deployment)

## Building the Project

To build the project, run:

```bash
mvn clean package
```

This will compile the project, generate the gRPC stubs, and create an executable JAR file.

## Running the System

### Using Docker Compose

The easiest way to run a multi-node cluster is using Docker Compose:

1. Build and start the cluster:
```bash
docker-compose up --build
```

This will start:
- 4 Raft nodes in a cluster (node1, node2, node3, node4)
- 1 standalone node (node5) that can be added to the cluster later
- 1 client node for interacting with the cluster

The nodes are accessible on the following ports:
- node1: 5050
- node2: 5051
- node3: 5052
- node4: 5053
- node5: 5054

### Manual Execution

To run a server node:

```bash
java -cp app.jar org.raft.server.Server <nodeId> <nodeAddress> [peerList]
```

To run a client:

```bash
java -cp app.jar org.raft.client.Client <serverAddress>
```

You can also run it using the `run.bat` on Windows:
```bat
./run.bat
```
or `run.sh` on Linux:
```bash
chmod +x run.sh
./run-raft.sh
```

## Architecture

### Protocol Buffers

The system uses two main protobuf service definitions:

1. `raft_service.proto`: Defines the core Raft protocol messages
   - RequestVote RPC
   - AppendEntries RPC
   - Log entry structure

2. `kv_store_service.proto`: Defines the key-value store operations
   - Client commands (GET, SET, DEL, etc.)
   - Log replication
   - Membership changes

### Components

- `RaftNode`: Core implementation of the Raft consensus protocol
- `Server`: gRPC server that handles both Raft protocol and key-value store operations
- `Client`: Command-line client for interacting with the cluster
- `KVStoreService`: Implementation of the key-value store operations
- `Peer`: Manages communication with other nodes in the cluster

## Client Commands
### Connection Commands

| Command | Description |
|---------|-------------|
| `connect <host:port>` | Connect to a specific server node |
| `exit` | Exit the client |
| `quit` | Exit the client (alternative) |

### Key-Value Store Operations

| Command | Description |
|---------|-------------|
| `GET <key>` | Retrieve value for a key |
| `SET <key> <value>` | Set value for a key |
| `DEL <key>` | Delete a key-value pair |
| `STRLEN <key>` | Get the length of a stored value |
| `APPEND <key> <value>` | Append value to an existing key |
| `PING` | Check if server is responsive |

### Cluster Management Commands

| Command | Description |
|---------|-------------|
| `REQUEST_LOG` | View the current log entries in the cluster |
| `ADD_MEMBER <nodeId> <nodeAddress>` | Add a new node to the cluster |
| `REMOVE_MEMBER <nodeId>` | Remove a node from the cluster |

### Example Usage

1. Basic key-value operations:
```bash
# Set a value
SET mykey Hello

# Get a value
GET mykey

# Append to existing value
APPEND mykey World

# Get string length
STRLEN mykey

# Delete a key
DEL mykey
```

2. Cluster management:
```bash
# View current log state
REQUEST_LOG

# Add a new node
ADD_MEMBER node5 node5:5050

# Remove a node
REMOVE_MEMBER node5
```

3. Changing connection:
```bash
# Connect to a different node
connect node2:5051
```

The client will automatically handle redirects to the current leader node when necessary. If a command fails due to a node not being the leader, the client will attempt to redirect the request to the correct leader node (up to 5 redirect attempts).

## Safety Guarantees

This implementation provides the following safety properties as specified in the Raft paper:

- Election Safety: at most one leader can be elected in a given term
- Leader Append-Only: a leader never overwrites or deletes entries in its log
- Log Matching: if two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index
- Leader Completeness: if a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms
- State Machine Safety: if a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index
