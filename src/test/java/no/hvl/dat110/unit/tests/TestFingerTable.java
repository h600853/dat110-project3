package no.hvl.dat110.unit.tests;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

class TestFingerTable {
	
	static List<String> fingerTableProcess1;
	
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
		
		expectedRoutingTable();
	}

	@Test
	void testFixFingerTable() throws RemoteException {
		
		NodeInterface process1 = Util.getProcessStub("process1", 9091);
		
		List<String> fingerTableActual = new ArrayList<>();
		List<NodeInterface> fingers = process1.getFingerTable();
		for(int i=0; i<fingers.size(); i++) {
			fingerTableActual.add(fingers.get(i).getNodeName());
		}
		Assertions.assertArrayEquals(fingerTableProcess1.toArray(), fingerTableActual.toArray());
	}
	
	private static void expectedRoutingTable() {
		fingerTableProcess1 = new ArrayList<>();
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process3");
		fingerTableProcess1.add("process5");
		fingerTableProcess1.add("process5");
		fingerTableProcess1.add("process4");
		fingerTableProcess1.add("process2");
	}

}
