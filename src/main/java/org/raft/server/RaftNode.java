package org.raft.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.raft.kvstore.rpc.ClientRequest;
import org.raft.kvstore.rpc.ClientResponse;
import org.raft.kvstore.rpc.MemberChangeArgs;
import org.raft.kvstore.rpc.MemberChangeReply;
import org.raft.kvstore.rpc.RequestLogReply;
import org.raft.raft.rpc.AppendEntriesArgs;
import org.raft.raft.rpc.AppendEntriesReply;
import org.raft.raft.rpc.LogEntry;
import org.raft.raft.rpc.RequestVoteArgs;
import org.raft.raft.rpc.RequestVoteReply;

public class RaftNode {
    private static final Logger logger = Logger.getLogger(RaftNode.class.getName());

    // Node configuration
    private final String nodeId;
    private final String selfAddress;
    private final Map<String, Peer> peers = new ConcurrentHashMap<>();

    // Persistent state on all servers
    private final AtomicLong currentTerm = new AtomicLong(0);
    private volatile String votedFor = null; // Made volatile for thread safety
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    // Volatile state on all servers
    private volatile NodeState currentState = NodeState.FOLLOWER;
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicLong currElectionTimeOut = new AtomicLong(0);
    private volatile String currentLeaderId = null; // Made volatile

    private final AtomicLong lastLeaderCommunicationTime = new AtomicLong(0);

    // State Machine (Key-Value Store)
    private final ConcurrentHashMap<String, String> keyValueStore = new ConcurrentHashMap<>();

    // Internal Timer and Scheduler
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private ScheduledFuture<?> electionTimeoutTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Random random = new Random();
    private static final int ELECTION_TIMEOUT_MIN = 2500;
    private static final int ELECTION_TIMEOUT_MAX = 4000;
    private static final int HEARTBEAT_INTERVAL_MS = 1000;

    // client request map for completable future
    private final Map<Long, CompletableFuture<ClientResponse>> clientRequestFutures = new ConcurrentHashMap<>();

    // client membership change map
    private final Map<Long, CompletableFuture<MemberChangeReply>> clientMemberChangeFutures = new ConcurrentHashMap<>();

    // executor for election threads
    private final ExecutorService electionRpcExecutor = Executors.newCachedThreadPool(
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "election-rpc-" + nodeId + "-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }
    );

    // executor for appendEntries threads
    private final ExecutorService appendEntriesRpcExecutor = Executors.newCachedThreadPool(
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "append-entries-" + nodeId + "-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }
    );

    private static final long APPEND_ENTRIES_RETRY_DELAY_MS = 100;

    // state for membership change
    private volatile Set<String> stableConfig;
    private volatile Set<String> oldConfig;
    private volatile Set<String> newConfig;
    private volatile boolean inJointConsensus = false;
    private Map<String, String> pendingNewPeerAddresses = new ConcurrentHashMap<>();

    // constructor
    public RaftNode(String nodeId, String selfAddress, List<String> peerAddresses) {
        this.nodeId = nodeId;
        this.selfAddress = selfAddress;

        this.stableConfig = new CopyOnWriteArraySet<>();
        this.stableConfig.add(nodeId);
        for (String peerAddress : peerAddresses) {
            String[] parts = peerAddress.split(",");
            String peerId = parts[0];
            String peerAddr = parts[1];
            if (!peerId.equals(nodeId)) {
                this.peers.put(peerId, new Peer(peerId, peerAddr));
                this.stableConfig.add(peerId);
            }
        }

        // placeholder entry
        log.add(LogEntry.newBuilder().setTerm(0).setType(LogEntry.LogType.SENTINEL).build());
        resetElectionTimer();
    }

    /* STATE TRANSITION */
    // method to become follower
    private synchronized void becomeFollower(long term) {
        boolean termIncreased = term > currentTerm.get();
        boolean wasNotFollower = currentState != NodeState.FOLLOWER;

        if (termIncreased || wasNotFollower) {
            logger.log(Level.INFO, "{0} becoming follower for term {1} (was {2}, oldTerm: {3})", new Object[]{nodeId, term, currentState, currentTerm.get()});
        }

        if (term > currentTerm.get()) {
            currentTerm.set(term);
            votedFor = null;
            currentLeaderId = null; // Leader for new term is unknown
        } else if (term < currentTerm.get()) {
            logger.log(Level.WARNING, "{0} tried to become follower with older term {1}, current is {2}", new Object[]{nodeId, term, currentTerm.get()});
            return;
        }
        currentState = NodeState.FOLLOWER;

        if (heartbeatTask != null && !heartbeatTask.isDone()) {
            heartbeatTask.cancel(false);
        }
        resetElectionTimer();
    }

    // method to become candidate
    private synchronized void becomeCandidate() {
        logger.log(Level.INFO, "{0} becoming Candidate for new term.", nodeId);
        currentState = NodeState.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor = nodeId;
        lastLeaderCommunicationTime.set(0);
        currentLeaderId = null;
        resetElectionTimer();
        startElection();
    }

    // method to become leader
    private synchronized void becomeLeader() {
        if (currentState != NodeState.CANDIDATE) {
            logger.log(Level.WARNING, "{0} tried to become leader but was not candidate. State: {1}", new Object[]{nodeId, currentState});
            return;
        }
        logger.log(Level.INFO, "{0} becoming Leader for term {1}", new Object[]{nodeId, currentTerm.get()});
        currentState = NodeState.LEADER;
        currentLeaderId = nodeId;
        votedFor = null;

        lastLeaderCommunicationTime.set(System.currentTimeMillis());

        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(true);
        }

        long lastLogIdx = log.size() - 1;
        for (Peer peer : peers.values()) {
            peer.setNextIndex(lastLogIdx + 1);
            peer.setNodeMatchIndex(0);
        }
        sendHeartbeats();
        startHeartbeatTimer();
    }

    /* TIMER */
    // method to reset election timeout schedule
    private void resetElectionTimer() {
//        logger.info(nodeId + " reset election timer");
        if (electionTimeoutTask != null && !electionTimeoutTask.isDone()) {
            electionTimeoutTask.cancel(false);
        }
        if (currentState == NodeState.FOLLOWER || currentState == NodeState.CANDIDATE) {
            long timeout = ELECTION_TIMEOUT_MIN + random.nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
            currElectionTimeOut.set(timeout);
            electionTimeoutTask = scheduler.schedule(this::handleElectionTimeout, timeout, TimeUnit.MILLISECONDS);
        }
    }

    // method to handle election timeout
    private void handleElectionTimeout() {
        if (currentState == NodeState.FOLLOWER || currentState == NodeState.CANDIDATE) {
            logger.log(Level.INFO, "{0} election timed out, starting new election.", nodeId);
            becomeCandidate();
        }
    }

    // method to start heartbeat schedule
    private void startHeartbeatTimer() {
        if (heartbeatTask != null && !heartbeatTask.isDone()) {
            heartbeatTask.cancel(true);
        }
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats,
                0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /* ELECTION RELATED METHODS */
    // start election for candidates
    private void startElection() {
        logger.log(Level.INFO, "{0} starts a new election", nodeId);
        final long term = currentTerm.get();
        final long lastLogIdx = log.size() - 1;
        final long lastLogTermVal = (lastLogIdx >= 0 && lastLogIdx < log.size()) ? log.get((int) lastLogIdx).getTerm() : 0;

        RequestVoteArgs request = RequestVoteArgs.newBuilder()
                .setTerm(term)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIdx)
                .setLastLogTerm(lastLogTermVal)
                .build();

        AtomicInteger votesReceived = new AtomicInteger(1);
        AtomicInteger votesOld = new AtomicInteger(0);
        AtomicInteger votesNew = new AtomicInteger(0);
        final boolean inTransitionChange = this.inJointConsensus;
        final Set<String> currOldConfig = inTransitionChange ? new HashSet<>(this.oldConfig) : new HashSet<>();
        final Set<String> currNewConfig = inTransitionChange ? new HashSet<>(this.newConfig) : new HashSet<>();

        if (inTransitionChange) {
            if (currOldConfig.contains(nodeId)) {
                votesOld.incrementAndGet();
            }
            if (currNewConfig.contains(nodeId)) {
                votesNew.incrementAndGet();
            }
        }

        // Handle single node case
        if (peers.isEmpty()) {
            synchronized (this) {
                if (currentState == NodeState.CANDIDATE && currentTerm.get() == term) {
                    becomeLeader();
                }
            }
            return;
        }

        for (Peer peer : peers.values()) {
            if (currentState != NodeState.CANDIDATE || currentTerm.get() != term) {
                return;
            }

            CompletableFuture.runAsync(() -> {
                try {
                    if (currentState != NodeState.CANDIDATE || currentTerm.get() != term) {
                        return;
                    }
                    RequestVoteReply reply = peer.getBlockingStub().withDeadlineAfter(currElectionTimeOut.get(), TimeUnit.MILLISECONDS).requestVote(request);
                    synchronized (RaftNode.this) {
                        logger.log(Level.INFO, "{0} received vote response from reply", nodeId);
                        if (reply.getTerm() > currentTerm.get()) {
                            becomeFollower(reply.getTerm());
                            return;
                        }

                        if (currentState == NodeState.CANDIDATE && currentTerm.get() == term && reply.getTerm() == term) {
                            if (reply.getVoteGranted()) {
                                votesReceived.incrementAndGet();
                                if (inTransitionChange) {
                                    if (currOldConfig.contains(peer.getNodeId())) {
                                        votesOld.incrementAndGet();
                                    }
                                    if (currNewConfig.contains(peer.getNodeId())) {
                                        votesNew.incrementAndGet();
                                    }
                                }
                                if (checkWin(votesReceived, votesOld, votesNew, term, inTransitionChange, currOldConfig, currNewConfig)) {
                                    logger.log(Level.INFO, "{0} become leader.", nodeId);
                                    becomeLeader();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.FINE, "{0} election RPC to {1} failed: {2}", new Object[]{nodeId, peer.getNodeId(), e.getMessage()});
                }
            }, electionRpcExecutor);
        }
    }

    private synchronized boolean checkWin(AtomicInteger totalVote, AtomicInteger oldVotes, AtomicInteger newVotes, long term, boolean isInTransition, Set<String> oldSnapshot, Set<String> newSnapshot) {
        if (currentState != NodeState.CANDIDATE || currentTerm.get() != term) {
            return false;
        }
        if (isInTransition) {
            int oldsize = oldSnapshot.size();
            int newSize = newSnapshot.size();

            if (oldsize == 0 && newSize == 0) {
                // unreachable i think
                return false;
            }

            boolean winOld = (oldVotes.get() >= oldsize / 2 + 1);
            boolean winNew = (newVotes.get() >= newSize / 2 + 1);
            return winNew && winOld;
        } else {
            int stableSize = stableConfig.size();
            return (totalVote.get() >= stableSize / 2 + 1);
        }
    }

    // handle vote request
    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        logger.log(Level.FINE, "{0} received RequestVote from {1} for term {2} (my term: {3}, votedFor: {4})", new Object[]{nodeId, args.getCandidateId(), args.getTerm(), currentTerm.get(), votedFor});

        // Check leader communication timeout
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLeaderCommunicationTime.get() < ELECTION_TIMEOUT_MIN && currentLeaderId != null) {
            logger.log(Level.FINE, "{0} rejecting vote for {1} - recent leader communication", new Object[]{nodeId, args.getCandidateId()});
            return RequestVoteReply.newBuilder()
                    .setTerm(currentTerm.get())
                    .setVoteGranted(false)
                    .build();
        }

        boolean voteGranted = false;
        if (args.getTerm() < currentTerm.get()) {
            return RequestVoteReply.newBuilder().setTerm(currentTerm.get()).setVoteGranted(false).build();
        }

        if (args.getTerm() > currentTerm.get()) {
            becomeFollower(args.getTerm());
        }

        // check candidates log up-to-date
        boolean logOk = false;
        long myLastLogTerm = (!log.isEmpty()) ? log.getLast().getTerm() : 0;
        long myLastLogIndex = log.size() - 1;

        if (args.getLastLogTerm() > myLastLogTerm) {
            logOk = true;
        } else if (args.getLastLogTerm() == myLastLogTerm && args.getLastLogIndex() >= myLastLogIndex) {
            logOk = true;
        }

        if ((votedFor == null || votedFor.equals(args.getCandidateId())) && logOk) {
            votedFor = args.getCandidateId();
            voteGranted = true;
            resetElectionTimer();
            logger.log(Level.INFO, "{0} voting YES for {1} for term {2}", new Object[]{nodeId, args.getCandidateId(), args.getTerm()});
        } else {
            logger.log(Level.INFO, "{0} voting NO for {1} for term {2} (logOk: {3}, votedFor: {4})", new Object[]{nodeId, args.getCandidateId(), args.getTerm(), logOk, votedFor});
        }
        return RequestVoteReply.newBuilder().setTerm(currentTerm.get()).setVoteGranted(voteGranted).build();
    }

    // send heartbeat from leader
    private void sendHeartbeats() {
        synchronized (this) {
            if (currentState != NodeState.LEADER) {
                return;
            }
            logger.log(Level.FINE, "{0} sending heartbeats for term {1}", new Object[]{nodeId, currentTerm.get()});
            lastLeaderCommunicationTime.set(System.currentTimeMillis());
            logger.log(Level.FINE, "{0} sending heartbeats for term {1}", new Object[]{nodeId, currentTerm.get()});
            sendAppendEntries(true);
        }
    }

    /* APPEND ENTRIES RELATED METHODS */
    // send append entries to multiple peer
    private synchronized void sendAppendEntries(boolean isHeartBeat) {
        if (isHeartBeat) {
            logger.log(Level.INFO, "{0} sends heart beats", nodeId);
        }
        for (Peer peer : peers.values()) {
            sendAppendEntries(peer, isHeartBeat);
        }
    }

    private void sendAppendEntries(Peer peer, boolean isHeartBeat) {
        if (currentState != NodeState.LEADER) return;
        final long nextIdx = peer.getNextIndex();
        final long currentTermSnapshot = currentTerm.get();
        final long currLogSize = log.size();
        long prevLogIdx = Math.max(0, nextIdx - 1);

        if (prevLogIdx >= currLogSize) {
            peer.setNextIndex(currLogSize);
            prevLogIdx = Math.max(0, currLogSize - 1);
        }

        long prevLogTerm = log.get((int) prevLogIdx).getTerm();
        AppendEntriesArgs.Builder builder = AppendEntriesArgs.newBuilder().
                setTerm(currentTerm.get()).
                setLeaderId(nodeId).
                setPrevLogIndex(prevLogIdx).
                setPrevLogTerm(prevLogTerm).
                setLeaderCommit(commitIndex.get());

        List<LogEntry> entries = new ArrayList<>();
        if (!isHeartBeat) {
            for (int i = (int) prevLogIdx + 1; i < log.size(); i++) {
                entries.add(log.get(i));
            }
        }
        builder.addAllEntries(entries);
        AppendEntriesArgs request = builder.build();

        CompletableFuture.runAsync(() -> {
            try {
                if (currentState != NodeState.LEADER || currentTerm.get() != currentTermSnapshot) {
                    logger.log(Level.FINE, "{0} AE to {1} aborted before RPC: No longer leader or term changed.", new Object[]{nodeId, peer.getNodeId()});
                    return;
                }

                AppendEntriesReply reply = peer.getBlockingStub()
                        .withDeadlineAfter(ELECTION_TIMEOUT_MAX, TimeUnit.MILLISECONDS)
                        .appendEntries(request);

                synchronized (this) {
                    if (currentState != NodeState.LEADER || currentTerm.get() != currentTermSnapshot) {
                        logger.log(Level.INFO, "{0} Leader state/term changed while AE RPC to {1} was in flight. Ignoring reply.", new Object[]{nodeId, peer.getNodeId()});
                        return;
                    }

                    if (reply.getTerm() > currentTerm.get()) {
                        logger.log(Level.INFO, "{0} Peer {1} replied with newer term {2}. Becoming follower.", new Object[]{nodeId, peer.getNodeId(), reply.getTerm()});
                        becomeFollower(reply.getTerm());
                        return;
                    }

                    if (reply.getTerm() == currentTerm.get()) {
                        if (reply.getSuccess()) {
                            long matchIndexFromThisRpc = request.getPrevLogIndex() + request.getEntriesCount();
                            long nextIndexFromThisRpc = matchIndexFromThisRpc + 1;

                            if (matchIndexFromThisRpc > peer.getNodeMatchIndex()) {
                                peer.setNodeMatchIndex(matchIndexFromThisRpc);
                            }
                            if (nextIndexFromThisRpc > peer.getNextIndex()) {
                                peer.setNextIndex(nextIndexFromThisRpc);
                            }

                            logger.log(Level.FINE, "{0} AE success from {1}. PeerMatchIndex now: {2}, PeerNextIndex now: {3}", new Object[]{nodeId, peer.getNodeId(), peer.getNodeMatchIndex(), peer.getNextIndex()});
                            updateCommitIndex();
                        } else if (!isHeartBeat) {
                            long newNextIdx = Math.max(1, peer.getNextIndex() - 1);
                            peer.setNextIndex(newNextIdx);
                            scheduler.schedule(() -> {
                                synchronized (this) {
                                    if (RaftNode.this.currentState == NodeState.LEADER && RaftNode.this.currentTerm.get() == currentTermSnapshot) {
                                        sendAppendEntries(peer, false);
                                    } else {
                                        logger.log(Level.INFO, "{0} Retry AE for {1} cancelled; state/term changed during delay.", new Object[]{nodeId, peer.getNodeId()});
                                    }
                                }
                            }, APPEND_ENTRIES_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            } catch (Exception e) {
                if (!isHeartBeat && currentState == NodeState.LEADER && currentTerm.get() == currentTermSnapshot) {
                    long newNextIdx = Math.max(1, peer.getNextIndex() - 1);
                    peer.setNextIndex(newNextIdx);
                    scheduler.schedule(() -> {
                        synchronized (this) {
                            if (RaftNode.this.currentState == NodeState.LEADER && RaftNode.this.currentTerm.get() == currentTermSnapshot) {
                                sendAppendEntries(peer, false);
                            } else {
                                logger.log(Level.INFO, "{0} Retry AE for {1} cancelled; state/term changed during delay.", new Object[]{nodeId, peer.getNodeId()});
                            }
                        }
                    }, APPEND_ENTRIES_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                }
            }
        }, appendEntriesRpcExecutor);
    }

    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        logger.log(Level.FINE, "{0} receives append entries from {1}", new Object[]{nodeId, args.getLeaderId()});
        // early return for old terms
        if (args.getTerm() < currentTerm.get()) {
            return AppendEntriesReply.newBuilder()
                    .setTerm(currentTerm.get())
                    .setSuccess(false)
                    .setMatchIndex(0)
                    .build();
        }

        // valid append entries received
        lastLeaderCommunicationTime.set(System.currentTimeMillis());
        currentLeaderId = args.getLeaderId();

        if (args.getTerm() > currentTerm.get() ||
                currentState == NodeState.CANDIDATE) {
            becomeFollower(args.getTerm());
        } else {
            resetElectionTimer();
        }

        // check log consistency
        if (args.getPrevLogIndex() >= log.size() || args.getPrevLogIndex() < 0 ||
                log.get((int) args.getPrevLogIndex()).getTerm() != args.getPrevLogTerm()) {
            return AppendEntriesReply.newBuilder()
                    .setTerm(currentTerm.get())
                    .setSuccess(false)
                    .setMatchIndex(0)
                    .build();
        }

        // Find conflicts and handle log truncation
        int conflictIndex = -1;
        int entriesStartIndex = (int) args.getPrevLogIndex() + 1;

        for (int i = 0; i < args.getEntriesCount(); i++) {
            int logIndex = entriesStartIndex + i;
            if (logIndex < log.size()) {
                if (log.get(logIndex).getTerm() != args.getEntries(i).getTerm()) {
                    conflictIndex = logIndex;
                    break;
                }
            } else {
                // No more entries in our log, no conflict
                break;
            }
        }

        // Delete conflicting entries and everything after
        if (conflictIndex != -1) {
            while (log.size() > conflictIndex) {
                log.removeLast();
            }
        }

        // Append new entries that aren't already in our log
        // Start appending from where our log ends
        int startAppendIndex = Math.max(0, log.size() - entriesStartIndex);

        for (int i = startAppendIndex; i < args.getEntriesCount(); i++) {
            log.add(args.getEntries(i));
        }

        // Check for configuration changes in the entries we just processed
        // We need to check ALL entries that were added to the log, not just the newly appended ones
        int configCheckStart = Math.max(0, Math.min(startAppendIndex, conflictIndex != -1 ? conflictIndex - entriesStartIndex : Integer.MAX_VALUE));

        for (int i = configCheckStart; i < args.getEntriesCount(); i++) {
            LogEntry entry = args.getEntries(i);
            if (entry.getType().equals(LogEntry.LogType.C_OLD_NEW)) {
                applyOldNewEntry(entry);
            }
            if (entry.getType().equals(LogEntry.LogType.C_NEW)) {
                applyNewEntry(entry);
            }
        }

        // update commit
        if (args.getLeaderCommit() > commitIndex.get()) {
            long newCommitIndex = Math.min(args.getLeaderCommit(), log.size() - 1);
            if (newCommitIndex > commitIndex.get()) {
                commitIndex.set(newCommitIndex);
                applyCommitedEntries();
            }
        }

        if (args.getEntriesCount() == 0) {
            logger.info("Received heart beat");
        } else {
            logger.info("Received append entries");
        }

        return AppendEntriesReply.newBuilder()
                .setTerm(currentTerm.get())
                .setSuccess(true)
                .setMatchIndex(log.size() - 1)
                .build();
    }

    // method to update commit index for leader
    private synchronized void updateCommitIndex() {
        if (currentState != NodeState.LEADER) return;

        for (long newCommitIndex = commitIndex.get() + 1; newCommitIndex < log.size(); newCommitIndex++) {
            LogEntry entry = log.get((int) newCommitIndex);
            if (entry.getTerm() != currentTerm.get()) {
                continue;
            }

            if (checkAcknowledgement(newCommitIndex)) {
                commitIndex.set(newCommitIndex);
            } else {
                break;
            }
        }

        if (commitIndex.get() > lastApplied.get()) {
            // apply committed entries
            applyCommitedEntries();
        }
    }

    // method to check if the majority of nodes has acknowledged this
    private synchronized boolean checkAcknowledgement(long targetLogIndex) {
        if (currentState != NodeState.LEADER) {
            return false;
        }

        if (inJointConsensus) {
            if (oldConfig.isEmpty() || newConfig.isEmpty()) {
                return false;
            }
            int oldAcks = 0;
            int newAcks = 0;
            if (oldConfig.contains(nodeId)) {
                oldAcks++;
            }
            if (newConfig.contains(nodeId)) {
                newAcks++;
            }
            for (String peerId : oldConfig) {
                if (!peerId.equals(nodeId) && peers.containsKey(peerId) && peers.get(peerId).getNodeMatchIndex() >= targetLogIndex) {
                    oldAcks++;
                }
            }
            for (String peerId : newConfig) {
                if (!peerId.equals(nodeId) && peers.containsKey(peerId) && peers.get(peerId).getNodeMatchIndex() >= targetLogIndex) {
                    newAcks++;
                }
            }
            boolean isOldMajority = oldAcks >= (oldConfig.size() / 2 + 1);
            boolean isNewMajority = newAcks >= (newConfig.size() / 2 + 1);
            logger.log(Level.INFO, "{0} {1}", new Object[]{oldAcks, newAcks});
            return isNewMajority && isOldMajority;
        } else {
            if (stableConfig.isEmpty()) {
                // this should be unreachable i guess
                return false;
            }

            int ack = 0;
            if (stableConfig.contains(nodeId)) {
                ack++;
            }
            for (String peerId : stableConfig) {
                if (!peerId.equals(nodeId) && peers.containsKey(peerId) && peers.get(peerId).getNodeMatchIndex() >= targetLogIndex) {
                    ack++;
                }
            }
            return ack >= (stableConfig.size() / 2 + 1);
        }
    }

    // method to apply commited entries
    private synchronized void applyCommitedEntries() {
        logger.log(Level.INFO, "{0} apply committed entries", nodeId);
        while (lastApplied.get() < commitIndex.get()) {
            long applyIdx = lastApplied.incrementAndGet();
            if (applyIdx >= log.size()) {
                lastApplied.decrementAndGet(); // revert
                break;
            }
            LogEntry currLog = log.get((int) applyIdx);
            String res;
            boolean success = true;

            switch (currLog.getType()) {
                case C_OLD_NEW -> {
                    applyCommitedOldNewEntry(currLog);
                    completeChangeMembershipFuture(applyIdx);
                }
                case C_NEW -> {
                    applyCommitedNewEntry(currLog);
                    completeChangeMembershipFuture(applyIdx);
                }
                default -> {
                    res = applyCommand(currLog.getType().name(), currLog.getKey(), currLog.getValue());
                    if (res.startsWith("ERROR")) {
                        success = false;
                    }   completeClientFuture(applyIdx, res, success);
                }
            }
        }
    }

    private synchronized String applyCommand(String cmd, String key, String value) {
        if (cmd != null) {
            return switch (cmd) {
                case "GET" -> {
                    if (key != null && !key.isEmpty()) {
                        yield keyValueStore.getOrDefault(key, "ERROR: Key not found");
                    } else {
                        yield "ERROR: Missing key for GET";
                    }
                }
                case "SET" -> {
                    if (key != null && value != null && !key.isEmpty()) {
                        keyValueStore.put(key, value);
                        yield "OK";
                    }
                    yield "ERROR: Missing key or value for SET";
                }
                case "STRLEN" -> {
                    if (key != null && !key.isEmpty()) {
                        yield String.valueOf(keyValueStore.getOrDefault(key, "").length());
                    } else {
                        yield "ERROR: Missing key for STRLEN";
                    }
                }
                case "DEL" -> {
                    if (key != null && !key.isEmpty()) {
                        String val = keyValueStore.remove(key);
                        yield val != null ? val : "";
                    }
                    yield "ERROR: Missing key for DEL";
                }
                case "APPEND" -> {
                    if (key != null && value != null && !key.isEmpty()) {
                        keyValueStore.compute(key, (k, v) -> (v == null) ? value : v + value);
                        yield "OK";
                    }
                    yield "ERROR: Missing key or value for APPEND";
                }
                default -> "ERROR: Unknown command '" + cmd + "'";
            };
        }
        return "ERROR: Missing command";
    }

    /* CLIENT REQUEST HANDLING */
    public ClientResponse handleClientExecute(ClientRequest request) {
        logger.info("handleClientExecute");
        final ClientRequest.CommandType type = request.getType();
        final String key = request.getKey();
        final String value = request.getValue();
        final String cmd = type.name();

        CompletableFuture<ClientResponse> responseFuture = new CompletableFuture<>();
        long tmp;
        synchronized (this) {
            if (currentState != NodeState.LEADER) {
                String leaderAddr = getPeerAddress(getCurrentLeaderId());
                ClientResponse redirectResponse = ClientResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderAddress(!leaderAddr.isEmpty() ? leaderAddr : "")
                        .build();
                responseFuture.complete(redirectResponse);
                logger.warning("This node is not a leader");
                return redirectResponse;
            }

            // if ping just response
            if (type == ClientRequest.CommandType.PING) {
                String leaderAddr = getPeerAddress(getCurrentLeaderId());
                String res = "PONG";
                ClientResponse pongResponse = ClientResponse.newBuilder()
                        .setSuccess(true)
                        .setLeaderAddress(!leaderAddr.isEmpty() ? leaderAddr : "")
                        .setResult(res)
                        .build();
                responseFuture.complete(pongResponse);
                logger.info("This is ping");
                return pongResponse;
            } else {
                LogEntry newEntry = LogEntry.newBuilder()
                        .setTerm(currentTerm.get())
                        .setKey(key)
                        .setValue(value)
                        .setType(LogEntry.LogType.valueOf(cmd))
                        .build();
                log.add(newEntry);
                long entryIndex = log.size() - 1;
                tmp = entryIndex;
                clientRequestFutures.put(entryIndex, responseFuture);
                sendAppendEntries(false);
            }
        }

        try {
            return responseFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            clientRequestFutures.remove(tmp);
            return ClientResponse.newBuilder().setSuccess(false).setResult("ERROR: " + e.getClass().getSimpleName() + " - " + e.getMessage()).build();
        }
    }

    public RequestLogReply handleRequestLog() {
        logger.info("handle request log");
        synchronized (this) {
            if (currentState != NodeState.LEADER) {
                String leaderAddr = getPeerAddress(getCurrentLeaderId());
                logger.log(Level.INFO, "{0} (not Leader) received request log, redirecting to {1}", new Object[]{nodeId, currentLeaderId != null ? currentLeaderId : "unknown leader"});
                return RequestLogReply.newBuilder().setSuccess(false).setLeaderAddress(!leaderAddr.isEmpty() ? leaderAddr : "").build();
            }
            return RequestLogReply.newBuilder()
                    .setSuccess(true)
                    .setLeaderAddress(getSelfAddress())
                    .addAllLogs(new ArrayList<>(log)) // Send a copy
                    .build();
        }
    }

    private void completeClientFuture(long logIndex, String result, boolean success) {
        CompletableFuture<ClientResponse> future = clientRequestFutures.remove(logIndex);
        if (future != null) {
            ClientResponse.Builder responseBuilder = ClientResponse.newBuilder()
                    .setSuccess(success)
                    .setResult(result != null ? result : "");
            if (!success && currentLeaderId != null && !currentLeaderId.equals(nodeId)) {
                responseBuilder.setLeaderAddress(getPeerAddress(currentLeaderId));
            }
            future.complete(responseBuilder.build());
        }
    }

    private void completeChangeMembershipFuture(long logIndex) {
        CompletableFuture<MemberChangeReply> future = clientMemberChangeFutures.remove(logIndex);
        if (future != null) {
            MemberChangeReply reply = MemberChangeReply.newBuilder().setSuccess(true).setErrorMessage("".isEmpty() ? "" : "").build();
            future.complete(reply);
        }
    }

    /* MEMBERSHIP CHANGE RELATED FUNCTIONS */
    public MemberChangeReply handleChangeMembership(MemberChangeArgs request) {
        final String type = request.getType().name();
        final String newNodeId = request.getNodeId();
        final String newNodeAddress = request.getNodeAddress();

        CompletableFuture<MemberChangeReply> responseFuture = new CompletableFuture<>();
        long tmp;
        synchronized (this) {
            if (currentState != NodeState.LEADER) {
                MemberChangeReply res = MemberChangeReply.newBuilder().setSuccess(false).setErrorMessage("ERROR: This node is not a leader").build();
                responseFuture.complete(res);
                logger.warning("This node is not a leader");
                return res;
            }

            if (inJointConsensus) {
                MemberChangeReply res = MemberChangeReply.newBuilder().setSuccess(false).setErrorMessage("ERROR: Another config change already in progress").build();
                responseFuture.complete(res);
                logger.warning("Another config change already in progress");
                return res;
            }

            // get old config
            Set<String> confOld = new HashSet<>(stableConfig);
            Set<String> confNew = new HashSet<>(stableConfig);
            pendingNewPeerAddresses.clear();

            if (type.equals("ADD")) {
                confNew.add(newNodeId);
                Peer newPeer = new Peer(newNodeId, newNodeAddress);
                newPeer.setNextIndex(log.size());
                newPeer.setNodeMatchIndex(0);
                this.peers.put(newNodeId, newPeer);
            } else {
                confNew.remove(newNodeId);
            }
            for (Peer peer : peers.values()) {
                pendingNewPeerAddresses.put(peer.getNodeId(), peer.getAddress());
            }
            this.oldConfig = confOld;
            this.newConfig = confNew;
            this.inJointConsensus = true;

            // create node map
            ArrayList<String> nodeMap = new ArrayList<>();
            nodeMap.add(this.nodeId + "=" + this.selfAddress);

            for (Peer peer : peers.values()) {
                nodeMap.add(peer.getNodeId() + "=" + peer.getAddress());
            }

            LogEntry newEntry = LogEntry.newBuilder()
                    .setTerm(currentTerm.get())
                    .setType(LogEntry.LogType.C_OLD_NEW)
                    .addAllOldConf(new ArrayList<>(oldConfig))
                    .addAllNewConf(new ArrayList<>(newConfig))
                    .addAllNodeMap(nodeMap)
                    .build();
            log.add(newEntry);
            long entryIdx = log.size() - 1;
            tmp = entryIdx;
            clientMemberChangeFutures.put(entryIdx, responseFuture);
            sendAppendEntries(false);
        }

        try {
            return responseFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            clientMemberChangeFutures.remove(tmp);
            return MemberChangeReply.newBuilder().setSuccess(false).setErrorMessage("ERROR: failed to do member change").build();
        }
    }

    private synchronized void applyOldNewEntry(LogEntry entry) {
        logger.log(Level.INFO, "{0} is applying c_old_new", nodeId);
        Set<String> confOld = new HashSet<>(entry.getOldConfList());
        Set<String> confNew = new HashSet<>(entry.getNewConfList());

        this.inJointConsensus = true;
        this.oldConfig = confOld;
        this.newConfig = confNew;

        // construct nodeMap
        Map<String, String> nodeMap = new HashMap<>();
        for (String raw : entry.getNodeMapList()) {
            String[] parts = raw.split("=");
            String id = parts[0];
            String address = parts[1];
            nodeMap.put(id, address);
        }
        pendingNewPeerAddresses = nodeMap;

        // check if there's node in old that hasn't been connected yet
        for (String oldNode : confOld) {
            if (!peers.containsKey(oldNode) && !oldNode.equals(this.nodeId)) {
                Peer peer = new Peer(oldNode, nodeMap.get(oldNode));
                peer.setNextIndex(log.size());
                peer.setNodeMatchIndex(0);
                this.peers.put(oldNode, peer);
            }
        }

        // check if there's node in new that hasn't been connected yet
        for (String newNode : confNew) {
            if (!peers.containsKey(newNode) && !newNode.equals(this.nodeId)) {
                Peer peer = new Peer(newNode, nodeMap.get(newNode));
                peer.setNextIndex(log.size());
                peer.setNodeMatchIndex(0);
                this.peers.put(newNode, peer);
            }
        }
    }

    private synchronized void applyNewEntry(LogEntry entry) {
        logger.log(Level.INFO, "{0} is applying c_new", nodeId);
        Set<String> newConf = new HashSet<>(entry.getNewConfList());
        this.stableConfig = newConf;
        this.oldConfig = new HashSet<>();
        this.newConfig = new HashSet<>();
        this.inJointConsensus = false;
        this.pendingNewPeerAddresses.clear();

        if (!newConf.contains(this.nodeId)) {
            for (Peer peer : peers.values()) {
                peer.disconnect();
            }
            peers.clear();
        } else {
            Set<String> peersToRemove = new HashSet<>();
            for (String peerIdInMap : this.peers.keySet()) {
                if (!newConf.contains(peerIdInMap) && !peerIdInMap.equals(this.nodeId)) {
                    peersToRemove.add(peerIdInMap);
                }
            }
            for (String peerToRemove : peersToRemove) {
                Peer p = this.peers.remove(peerToRemove);
                if (p != null) {
                    p.disconnect();
                    logger.log(Level.INFO, "{0} disconnected and removed peer {1} from map as it's not in C_new.", new Object[]{nodeId, peerToRemove});
                }
            }
        }
    }

    private synchronized void applyCommitedOldNewEntry(LogEntry entry) {
        if (currentState != NodeState.LEADER) {
            return;
        }
        logger.log(Level.INFO, "{0} is committing c_old_new", nodeId);

        if (this.inJointConsensus) {
            // send the new entry
            Set<String> newConf = new HashSet<>(entry.getNewConfList());

            LogEntry newEntry = LogEntry.newBuilder()
                    .setTerm(currentTerm.get())
                    .addAllNewConf(newConf)
                    .setType(LogEntry.LogType.C_NEW)
                    .build();
            log.add(newEntry);
            sendAppendEntries(false);
        }
    }

    private synchronized void applyCommitedNewEntry(LogEntry entry) {
        logger.log(Level.INFO, "{0} is committing c_new", nodeId);

        if (!inJointConsensus) return;
        Set<String> newConf = new HashSet<>(entry.getNewConfList());
        this.stableConfig = newConf;
        this.oldConfig = new HashSet<>();
        this.newConfig = new HashSet<>();
        this.inJointConsensus = false;

        if (!newConf.contains(this.nodeId)) {
            for (Peer peer : peers.values()) {
                peer.disconnect();
            }
            peers.clear();
            becomeFollower(currentTerm.get()); // step down from leadership
        } else {
            Set<String> peersToRemove = new HashSet<>();
            for (String peerIdInMap : this.peers.keySet()) {
                if (!newConf.contains(peerIdInMap) && !peerIdInMap.equals(this.nodeId)) {
                    peersToRemove.add(peerIdInMap);
                }
            }
            for (String peerToRemove : peersToRemove) {
                Peer p = this.peers.remove(peerToRemove);
                if (p != null) {
                    p.disconnect();
                    logger.log(Level.INFO, "{0} disconnected and removed peer {1}", new Object[]{nodeId, peerToRemove});
                }
            }
        }
    }

    /* UTILITIES */
    public String getNodeId() {
        return nodeId;
    }

    public String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public String getSelfAddress() {
        return selfAddress;
    }

    public NodeState getCurrentState() {
        return currentState;
    }

    private String getPeerAddress(String peerNodeId) {
        if (peerNodeId == null) return "";
        synchronized (this) {
            if (peerNodeId.equals(this.nodeId)) return this.selfAddress;
            Peer peer = peers.get(peerNodeId);
            if (peer != null) {
                return peer.getAddress();
            } else {
                return "";
            }
        }
    }

    public void shutdown() {
        logger.log(Level.INFO, "{0} shutting down...", nodeId);
        if (electionTimeoutTask != null) electionTimeoutTask.cancel(true);
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        scheduler.shutdownNow();
        electionRpcExecutor.shutdownNow();
        appendEntriesRpcExecutor.shutdownNow();
        peers.values().forEach(Peer::disconnect);
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.log(Level.WARNING, "{0} scheduler did not terminate cleanly.", nodeId);
            }
            if (!electionRpcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.log(Level.WARNING, "{0} electionRpcExecutor did not terminate cleanly.", nodeId);
            }
            if (!appendEntriesRpcExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.log(Level.WARNING, "{0} appendEntriesRpcExecutor did not terminate cleanly.", nodeId);
            }
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "{0} shutdown interrupted.", nodeId);
            Thread.currentThread().interrupt();
        }
        logger.log(Level.INFO, "{0} scheduler and peer connections shut down.", nodeId);
    }
}