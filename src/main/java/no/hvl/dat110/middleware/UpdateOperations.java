/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class UpdateOperations {

	private static final Logger logger = LogManager.getLogger(UpdateOperations.class);
	private Map<BigInteger, Message> filesMetadata;
	private NodeInterface node;
	/**
	 * 
	 */
	public UpdateOperations(NodeInterface node, Map<BigInteger, Message> filesMetadata) {
		this.node = node;
		this.filesMetadata = filesMetadata;
	}
	
	public void saveFileContent(String filename, BigInteger fileID, byte[] bytesOfFile, boolean primary) throws RemoteException {
		try {
			buildMessage(filename, fileID, bytesOfFile, primary);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	private void buildMessage(String filename, BigInteger fileID, byte[] bytesOfFile, boolean primary) throws RemoteException {
		
		Message message = new Message();
		message.setNodeID(node.getNodeID());
		message.setNodeName(node.getNodeName());
		message.setPort(node.getPort());
		message.setHashOfFile(fileID); 								// this is the hash of the replica of filename which could be any of filename + i (where i=0 to numreplica)
		message.setNameOfFile(filename);							// 
		message.setBytesOfFile(bytesOfFile);
		message.setPrimaryServer(primary); 							// variable for remote-write protocol

		filesMetadata.put(fileID, message);								// save the replica and its metadata
	}
	
	/**
	 * 
	 * @param updates
	 * @throws RemoteException
	 */
	public void updateFileContent(List<Message> updates) throws RemoteException {

		// now perform all updates
		updates.forEach(update -> {			
			try {
				logger.info("Update file is being performed by this peer: "+node.getNodeName()+" | "+update.getHashOfFile());
				Message mmeta = node.getFilesMetadata(update.getHashOfFile());
				mmeta.setBytesOfFile(update.getBytesOfFile());
			} catch(RemoteException e) {
				e.printStackTrace();
			}
		});

	}
	
	/**
	 * 
	 * @param updates bytes array
	 * @throws RemoteException
	 */
	public void broadcastUpdatetoPeers(Set<Message> activenodesforfile, byte[] updates) throws RemoteException {

		Map<String, List<Message>> pernode = buildPerNodeUpdates(activenodesforfile, updates);

		pernode.forEach((peer, allupdates) -> {
			
			logger.info("Trying to update file replicas for peer: "+peer);
			
			try {
				if(peer.equals(node.getNodeID().toString())) {
					updateFileContent(allupdates); 														// perform updates.. don't issue a remote call again
				} else {
					NodeInterface pnode = Util.getProcessStub(allupdates.get(0).getNodeName(), allupdates.get(0).getPort()); 		// get remote stub of peer
					pnode.updateFileContent(allupdates);  	// perform updates					
				}
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			logger.info("Finished updating file for peer: "+peer);
		});
	}
	
	/**
	 * if a node holds multiple replicas of the same file, all updates to these replicas must be
	 * performed with a single lock. So, we build per node updates
	 */
	public Map<String, List<Message>> buildPerNodeUpdates(Set<Message> activenodesforfile, byte[] bytesOfFile) throws RemoteException {
		
		Map<String, List<Message>> pernode = new HashMap<>();
		
		activenodesforfile.forEach(peer -> {
			try {
				if(pernode.containsKey(peer.getNodeID().toString())) {
					List<Message> allmsgs = pernode.get(peer.getNodeID().toString());
					Message newmsg = new Message();
					newmsg.setNodeName(peer.getNodeName());
					newmsg.setHashOfFile(peer.getHashOfFile());
					newmsg.setPort(peer.getPort());
					newmsg.setBytesOfFile(bytesOfFile);
					newmsg.setPrimaryServer(peer.isPrimaryServer());
					allmsgs.add(newmsg);
				} else { 
					List<Message> allmsgs = new ArrayList<>();
					Message newmsg = new Message();
					newmsg.setNodeName(peer.getNodeName());
					newmsg.setHashOfFile(peer.getHashOfFile());
					newmsg.setPort(peer.getPort());
					newmsg.setBytesOfFile(bytesOfFile);
					newmsg.setPrimaryServer(peer.isPrimaryServer());
					allmsgs.add(newmsg);
					pernode.put(peer.getNodeID().toString(), allmsgs);
				}
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		});
		
		return pernode;
	}

}
