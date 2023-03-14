package no.hvl.dat110.unit.tests;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;

class TestRemote {
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		// start processes . Processes will form a ring
		SetUp setup = SetUp.getInstance();
		if(!setup.isStarted()) {
			setup.startProcesses();
			Thread.sleep(12000); 			// let the ring stabilize before distributing files
			// distribute the files to the ring
			setup.doDistribute();
			Thread.sleep(5000); 			// let the ring stabilize before starting the test
		}
	}

	@Test
	void test() throws InterruptedException, RemoteException {
		
		/** This is a client **/
		/** start -> Client -> update(m) -> p1 -> replicasofm(m)[2,3,4] -> Client -> locatePrimary[2, 3, 4] -> primary=4
		 * -> update(m)[4] -> 4 -> distributeUpdate(m)[2, 3, 4] - > end **/
		// retrieve the process stubs to be contacted by the client
		NodeInterface p1 = Util.getProcessStub("process1", 9091);
		
		FileManager fm = new FileManager(p1, Util.numReplicas);
		String filename = "file3";
		String newupdate = "overwrite the content of this existing file - i.e. file3";
		
		Set<Message> activepeers = fm.requestActiveNodesForFile(filename);  // 
		
		/** Find the primary for file3 */
		NodeInterface primary = fm.findPrimaryOfItem();
		
		System.out.println("Primary = "+primary.getNodeName());
		
		// update file
		primary.requestRemoteWriteOperation(newupdate.getBytes(), activepeers);
		
		
		// retrieve the current file content from all replicas holding file3
		activepeers.forEach(peer -> {
			String name = peer.getNodeName();
			int port = peer.getPort();
			BigInteger fileid = peer.getHashOfFile();			
			NodeInterface p = Util.getProcessStub(name, port);
			
			Message m = null;
			try {
				m = p.getFilesMetadata(fileid);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
			assertArrayEquals(m.getBytesOfFile(), newupdate.getBytes());
			
		});
		
	
	}

}
