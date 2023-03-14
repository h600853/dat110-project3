package no.hvl.dat110.middleware;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.chordoperations.ChordProtocols;
import no.hvl.dat110.rpc.interfaces.NodeInterface;

/**
 * @author tdoy
 * dat110: Project 3
 */


public class NodeServer {
	
	private static final Logger logger = LogManager.getLogger(NodeServer.class);
	private String nodename;
	private int port;
	private NodeInterface chordnode;
	
	public NodeServer(String nodename, int port){
		this.nodename = nodename;
		this.port = port;
		start();										// start the server aka registry
		new ChordProtocols(chordnode);					// start the chord protocols
	}
	
	public NodeServer(String nodename, int port, boolean gui){	// this constructor is used for the GUI
		this.nodename = nodename;
		this.port = port;
		start();										// start the server aka registry
	}
	
	public void start() {
		
		try {		
			// create registry and start it on port 9091
			Registry registry = LocateRegistry.createRegistry(port);
			
			// Make a new instance (stub) of the implementation class
			chordnode = new Node(nodename, port);
			
			// Bind the remote object (stub) in the registry
			registry.bind(nodename, chordnode);
			
			logger.info(nodename+" server is running... ");

		}catch(Exception e) {
			logger.error("Node Server: "+e.getMessage());
		}
	}

	public NodeInterface getNode() {
		
		return chordnode;
	}

}
