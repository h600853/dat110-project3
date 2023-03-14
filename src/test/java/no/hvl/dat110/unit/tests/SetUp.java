/**
 * 
 */
package no.hvl.dat110.unit.tests;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import no.hvl.dat110.middleware.NodeServer;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 * Singleton class to ensure we start the 5 processes just once for all the tests 
 * that require the 5 processes to be running
 */
public class SetUp {

	private static SetUp INSTANCE = null;
	private boolean started = false;
	
	private SetUp() {
		// 
	}
	
	public static SetUp getInstance() {
		if(INSTANCE == null) {
			INSTANCE = new SetUp();
		}
		
		return INSTANCE;
	}
	
	public void startProcesses() throws InterruptedException {
		started = true;
		Thread.sleep(1000);
		new NodeServer("process1", 9091);
		Thread.sleep(2000);
		new NodeServer("process2", 9092);
		Thread.sleep(2000);
		new NodeServer("process3", 9093);
		Thread.sleep(2000);
		new NodeServer("process4", 9094);
		Thread.sleep(2000);
		new NodeServer("process5", 9095);
	}
	
	public void doDistribute() throws NoSuchAlgorithmException, IOException {
		// use this node to distribute files to active peers
		String path = "src/test/resources/files/";														// absolute path to the files
		String[] files = {"file1.txt","file2.txt","file3.txt","file4.txt","file5.txt"}; // we just limit to 5 files
		
		String node1 = "process1";														// this is the peer we want to use to resolve and distribute files
		NodeInterface p1 = Util.getProcessStub(node1, 9091);	 						// Look up the registry for the remote stub for process1
		
		FileManager fm = new FileManager(p1, Util.numReplicas);							// get the filemanager
		
		for(int i=0; i<files.length; i++) {												// iterate over the files and distribute them to the running nodes
			fm.setFilepath(path+files[i]);
			fm.readFile();
			fm.distributeReplicastoPeers();												// distribute the replicas to active peers
		}
	}

	public boolean isStarted() {
		return started;
	}
	
//	public static void main(String[] args) throws InterruptedException, NoSuchAlgorithmException, IOException {
//		startProcesses();
//		Thread.sleep(10000);
//		doDistribute();
//	}
}
