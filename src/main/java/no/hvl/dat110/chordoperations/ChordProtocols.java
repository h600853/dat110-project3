/**
 * 
 */
package no.hvl.dat110.chordoperations;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Set;
import java.util.Timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class ChordProtocols {
	
	private static final Logger logger = LogManager.getLogger(ChordProtocols.class);
	/**
	 * UpdateSuccessor
	 * StabilizeRing
	 * FixFingerTable
	 * CheckPredecessor
	 */
	
	private NodeInterface chordnode;
	private StabilizationProtocols stabprotocol;
	
	public ChordProtocols(NodeInterface chordnode) {
		this.chordnode = chordnode;
		joinRing();
		stabilizationProtocols();
	}
	
	/**
	 * Public access bcos it's used by the GUI
	 */
	public void stabilizationProtocols() {
		Timer timer = new Timer();
		stabprotocol = new StabilizationProtocols(this, timer);
		timer.scheduleAtFixedRate(stabprotocol, 5000, 2000);
	}
	
	/**
	 * Public access bcos it's used by the GUI
	 */
	public void joinRing() {
		
		//Registry registry = Util.tryIPs();											// try the trackers IP addresses
		try {
			Registry registry = Util.tryIPSingleMachine(chordnode.getNodeName());
 			// try the ports
			if(registry != null) {
				try {
					String foundNode = Util.activeIP;
	
					NodeInterface randomNode = (NodeInterface) registry.lookup(foundNode);
					
					logger.info("JoinRing-randomNode = "+randomNode.getNodeName());
					
					// call remote findSuccessor function. The result will be the successor of this randomNode
					NodeInterface chordnodeSuccessor = randomNode.findSuccessor(chordnode.getNodeID());
	
					// set the successor of this node to chordnodeSuccessor and its predecessor to null
					chordnode.setSuccessor(chordnodeSuccessor);	
					chordnode.setPredecessor(null);
					
					// notify chordnodeSuccessor of a new predecessor
					chordnodeSuccessor.notify(chordnode);
					
					// fix the finger table - create a new routing table for this node
					fixFingerTable();
					
					// copy all keys that are less or equal (<=) to chordnode ID to chordnode
					((Node) chordnode).copyKeysFromSuccessor(chordnodeSuccessor);
					
					logger.info(chordnode.getNodeName()+" is between null | "+chordnodeSuccessor.getNodeName());
					
				} catch (Exception e) {
					logger.error(e.getMessage());
				}
			} else {
				
				createRing((Node) chordnode);		// no node is available, create a new ring
			}
		} catch (NumberFormatException | RemoteException e1) {
			// TODO Auto-generated catch block
		}
	}
	
	private void createRing(Node node) throws RemoteException {
		
		// set predecessor to nil - No predecessor for now
		node.setPredecessor(null);
		
		// set the successor to itself
		node.setSuccessor(node);
		
		logger.info("New ring created. Node = "+node.getNodeName()+" | Successor = "+node.getSuccessor().getNodeName()+
				" | Predecessor = "+node.getPredecessor());
		
	}
	
	public void leaveRing() throws RemoteException {
		
		logger.info("Attempting to update successor and predecessor before leaving the ring...");
		
		try {
		 
			NodeInterface prednode = chordnode.getPredecessor();														// get the predecessor			
			NodeInterface succnode = chordnode.getSuccessor();														// get the successor		
			
			Set<BigInteger> keyids = chordnode.getNodeKeys();									// get the keys for chordnode
			
			if(succnode != null) {												// add chordnode's keys to its successor
				keyids.forEach(fileID -> {
					try {
						logger.info("Adding fileID = "+fileID+" to "+succnode.getNodeName());

						succnode.addKey(fileID);
						Message msg = chordnode.getFilesMetadata().get(fileID);				
						succnode.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer()); 			// save the file in memory of the newly joined node
					} catch (RemoteException e) {
						//
					} 
				});

				succnode.setPredecessor(prednode); 							// set prednode as the predecessor of succnode
			}
			if(prednode != null) {
				prednode.setSuccessor(succnode);							// set succnode as the successor of prednode			
			} 
			chordnode.setSuccessor(chordnode);
			chordnode.setPredecessor(chordnode);
			chordnode.getNodeKeys().clear();
			stabprotocol.setStop(true);
			
		}catch(Exception e) {
			//
			logger.error("some errors while updating succ/pred/keys...\n"+e.getMessage());
		}
		
		logger.info("Update of successor and predecessor completed...bye!");
	}
	
	public void fixFingerTable() {
		
		try {
			logger.info("Fixing the FingerTable for the Node: "+ chordnode.getNodeName());
	
			// get the finger table from the chordnode (list object)
			
			// ensure to clear the current finger table
			
			// get the address size from the Hash class. This is the modulus and our address space (2^mbit = modulus)
			
			// get the number of bits from the Hash class. Number of bits = size of the finger table
			
			// iterate over the number of bits			
			
			// compute: k = succ(n + 2^(i)) mod 2^mbit
			
			// then: use chordnode to find the successor of k. (i.e., succnode = chordnode.findSuccessor(k))
			
			// check that succnode is not null, then add it to the finger table

		} catch (RemoteException e) {
			//
		}
	}

	protected NodeInterface getChordnode() {
		return chordnode;
	}

}
