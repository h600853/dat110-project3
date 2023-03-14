package no.hvl.dat110.rpc.interfaces;

/**
 * dat110
 * @author tdoy
 *
 */

import java.math.BigInteger;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import no.hvl.dat110.middleware.Message;


public interface NodeInterface extends Remote {
	
	public BigInteger getNodeID() throws RemoteException;
	
	public Set<BigInteger> getNodeKeys() throws RemoteException;
	
	public String getNodeName() throws RemoteException;
	
	public int getPort() throws RemoteException;
	
	public void setSuccessor(NodeInterface succ) throws RemoteException;
	
	public void setPredecessor(NodeInterface pred) throws RemoteException;
	
	public NodeInterface getPredecessor() throws RemoteException;
	
	public NodeInterface getSuccessor() throws RemoteException;
	
	public List<NodeInterface> getFingerTable() throws RemoteException;
	
	public void addKey(BigInteger id) throws RemoteException;
	
	public void removeKey(BigInteger id) throws RemoteException;
	
	public NodeInterface findSuccessor(BigInteger key) throws RemoteException;
	
	public void notify(NodeInterface pred) throws RemoteException;
	
	public Message getFilesMetadata(BigInteger fileID) throws RemoteException;
	
	public Map<BigInteger, Message> getFilesMetadata() throws RemoteException;
	
	public void saveFileContent(String filename, BigInteger fileID, byte[] bytesOfFile, boolean primary) throws RemoteException;
	
	public void updateFileContent(List<Message> updates) throws RemoteException;
	
	public void broadcastUpdatetoPeers(byte[] bytesOfFile) throws RemoteException;

	/** Remote-Write Protocol */
	public void requestRemoteWriteOperation(byte[] updates, Set<Message> activenodes) throws RemoteException;
	
	/** Concerns mutual exclusion algorithm*/
	
	public boolean requestMutexWriteOperation(Message message, byte[] updates, Set<Message> messages) throws RemoteException;
	
	public void acquireLock() throws RemoteException;
	
	public void releaseLocks() throws RemoteException;
	
	public void multicastReleaseLocks(Set<Message> activenodes) throws RemoteException;
	
	public void onMutexAcknowledgementReceived(Message message) throws RemoteException;
	
	public void onMutexRequestReceived(Message message) throws RemoteException;
	
}
