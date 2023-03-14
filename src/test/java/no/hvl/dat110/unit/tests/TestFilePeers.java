package no.hvl.dat110.unit.tests;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;

class TestFilePeers {
	
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
		
		// retrieve the process stubs to be contacted for finding replicas of file2
		NodeInterface p3 = Util.getProcessStub("process3", 9093);
		
		FileManager fm = new FileManager(p3, Util.numReplicas);
		String filename = "file2";
		
		Set<Message> activepeers = fm.requestActiveNodesForFile(filename);  // 2, 3 ,4 are holding file2
		
		// retrieve the actual file names from the peers
		List<String> actualpeers = new ArrayList<>();
		activepeers.forEach(peer -> {
			actualpeers.add(peer.getNodeName());
			
		});
		
		List<String> expectedpeers = new ArrayList<>();
		expectedpeers.add("process2");
		expectedpeers.add("process3");
		expectedpeers.add("process4");
		expectedpeers.add("process4");
		
		
		// sort both lists
		
		Collections.sort(actualpeers);
		Collections.sort(expectedpeers);
		
		assertArrayEquals(expectedpeers.toArray(), actualpeers.toArray());
	
	}

}
