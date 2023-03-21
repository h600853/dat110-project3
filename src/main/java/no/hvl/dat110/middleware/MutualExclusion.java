package no.hvl.dat110.middleware;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 */
public class MutualExclusion {
    private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
    /**
     * lock variables
     */
    private boolean CS_BUSY = false;                        // indicate to be in critical section (accessing a shared resource)
    private boolean WANTS_TO_ENTER_CS = false;                // indicate to want to enter CS
    private final List<Message> queueack;                        // queue for acknowledged messages
    private final List<Message> mutexqueue;                        // queue for storing process that are denied permission. We really don't need this for quorum-protocol
    private final LamportClock clock;                                // lamport clock
    private final Node node;

    public MutualExclusion(Node node) {
        this.node = node;

        clock = new LamportClock();
        queueack = new ArrayList<>();
        mutexqueue = new ArrayList<>();
    }

    public synchronized void acquireLock() {
        CS_BUSY = true;
    }

    public void releaseLocks() {
        WANTS_TO_ENTER_CS = false;
        CS_BUSY = false;
    }

    public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
        // Log that the node wants to access the critical section
        logger.info(node.nodename + " wants to access CS");

        // Clear the queues before starting the mutual exclusion algorithm
        queueack.clear();
        mutexqueue.clear();

        // Increment the Lamport clock
        clock.increment();

        // Set the clock on the message
        message.setClock(clock.getClock());

        // Indicate that the node wants to access the resource
        WANTS_TO_ENTER_CS = true;

        // Remove duplicates from the list of active nodes
        List<Message> messages = removeDuplicatePeersBeforeVoting();

        // Multicast the message to the active nodes
        multicastMessage(message, messages);

        // Wait for acknowledgements from all active nodes
        if (areAllMessagesReturned(messages.size())) {
            // Acquire the lock
            acquireLock();

            // Broadcast updates to all peers
            node.broadcastUpdatetoPeers(updates);

            // Clear the mutexqueue
            mutexqueue.clear();

            // Return permission to enter the critical section
            return true;
        }

        // Permission to enter the critical section was denied
        return false;
    }


    // multicast message to other processes including self
    private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
        // iterate over the activenodes
        for (Message m : activenodes) {
            // obtain a stub for each node from the registry
            NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());

            // call onMutexRequestReceived()
            if (stub != null) {
                stub.onMutexRequestReceived(message);
            }
        }

        logger.info("Number of peers to vote = " + activenodes.size());
    }


    public void onMutexRequestReceived(Message message) throws RemoteException {
        // increment the local clock
        // if message is from self, acknowledge, and call onMutexAcknowledgementReceived()

        if (message.getNodeName().equals(node.nodename)) {
            message.setAcknowledged(true);
            onMutexAcknowledgementReceived(message);
        }

        /* write if statement to transition to the correct caseid */
        // caseid=0: Receiver is not accessing shared resource and does not want to (send OK to sender)
        // caseid=1: Receiver already has access to the resource (dont reply but queue the request)
        // caseid=2: Receiver wants to access resource but is yet to - compare own message clock to received message's clock

        int caseid = (!CS_BUSY && !WANTS_TO_ENTER_CS) ? 0 : CS_BUSY ? 1 : 2;

        // check for decision
        doDecisionAlgorithm(message, mutexqueue, caseid);
    }

    public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
        String procName = message.getNodeName();
        int port = message.getPort();

        switch (condition) {
            /** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
            case 0: {
                // get a stub for the sender from the registry
                // acknowledge message
                // send acknowledgement back by calling onMutexAcknowledgementReceived()

                NodeInterface stub = Util.getProcessStub(procName, port);
                message.setAcknowledged(true);
                if (stub != null) {
                    stub.onMutexRequestReceived(message);
                }

                break;
            }

            /** case 2: Receiver already has access to the resource (dont reply but queue the request) */
            case 1: {
                // queue this message
                queue.add(message);
                break;
            }

            /**
             *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
             *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
             */
            case 2: {
                // check the clock of the sending process (note that the correct clock is in the message)
                // own clock for the multicast message (note that the correct clock is in the message)
                // compare clocks, the lowest wins
                // if clocks are the same, compare nodeIDs, the lowest wins
                // if sender wins, acknowledge the message, obtain a stub and call onMutexAcknowledgementReceived()
                // if sender looses, queue it

                int messageClock = message.getClock();
                int ownClock = clock.getClock();

                if (messageClock == ownClock) {
                    if (message.getNodeID().compareTo(node.getNodeID()) < 0) {
                        message.setAcknowledged(true);
                        NodeInterface stub = Util.getProcessStub(procName, port);
                        if (stub != null) {
                            stub.onMutexAcknowledgementReceived(message);
                        }
                    } else {
                        queue.add(message);
                    }
                } else if (messageClock < ownClock) {
                    message.setAcknowledged(true);
                    NodeInterface stub = Util.getProcessStub(procName, port);
                    if (stub != null) {
                        stub.onMutexAcknowledgementReceived(message);
                    }
                } else {
                    queue.add(message);
                }
                break;
            }

            default:
                break;
        }
    }

    public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
        // add message to queueack

        queueack.add(message);
    }

    // multicast release locks message to other processes including self
    public void multicastReleaseLocks(Set<Message> activenodes) {
        logger.info("Releasing locks from = " + activenodes.size());

        // iterate over the activenodes
        // obtain a stub for each node from the registry
        // call releaseLocks()

        for (Message m : activenodes) {
            NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());

            try {
                if (stub != null) {
                    stub.releaseLocks();
                }
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
        logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());

        // check if the size of the queueack is same as the numvoters
        // clear the queueack
        // return true if yes and false if no

        if (queueack.size() == numvoters) {
            queueack.clear();
            return true;
        }

        return false;
    }

    private List<Message> removeDuplicatePeersBeforeVoting() {
        List<Message> uniquepeer = new ArrayList<>();
        for (Message p : node.activenodesforfile) {
            boolean found = false;
            for (Message p1 : uniquepeer) {
                if (p.getNodeName().equals(p1.getNodeName())) {
                    found = true;
                    break;
                }
            }
            if (!found) uniquepeer.add(p);
        }
        return uniquepeer;
    }
}