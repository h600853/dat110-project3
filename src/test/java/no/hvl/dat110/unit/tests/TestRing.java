/**
 * 
 */
package no.hvl.dat110.unit.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
class TestRing {
	
	static Map<String, BigInteger> process;
	static Map<String, String> esucclist;
	static Map<String, String> epredlist;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		
		// we use MD5 = 128bits digest e.g. MD5(process1) = 53937554629190552131995290006614509577
		process = new HashMap<>();
		process.put("process1", new BigInteger("53937554629190552131995290006614509577"));
		process.put("process2", new BigInteger("15618062003214643351512781541041391612"));
		process.put("process3", new BigInteger("66910184482037901621933403444034052414"));
		process.put("process4", new BigInteger("210821560651360572675896360671414673172"));
		process.put("process5", new BigInteger("121411138451101288395601026024677976156"));
		
		// processes as a ring (clockwise) - expected behaviour
		/*
		 *  process2--process1--process3
		 *  	|				 	|
		 * 	process4------------process5	
		 */
		
		// successor list
		esucclist = new HashMap<>();
		esucclist.put("process2", "process1");					// succ(process2) = process1
		esucclist.put("process1", "process3");
		esucclist.put("process3", "process5");
		esucclist.put("process5", "process4");
		esucclist.put("process4", "process2");
		
		// predecessor list
		epredlist = new HashMap<>();
		epredlist.put("process2", "process4");					// pred(process2) = process4
		epredlist.put("process1", "process2");
		epredlist.put("process3", "process1");
		epredlist.put("process5", "process3");
		epredlist.put("process4", "process5");
		// start processes . Processes will form a ring
		SetUp setup = SetUp.getInstance();
		if(!setup.isStarted()) {
			setup.startProcesses();
			Thread.sleep(12000); 			// let the ring stabilize before distributing files
		}

	}

	@Test
	void test() throws RemoteException, InterruptedException {
		
		// retrieve the processes stubs
		NodeInterface p1 = Util.getProcessStub("process1", 9091);
		NodeInterface p2 = Util.getProcessStub("process2", 9092);
		NodeInterface p3 = Util.getProcessStub("process3", 9093);
		NodeInterface p4 = Util.getProcessStub("process4", 9094);
		NodeInterface p5 = Util.getProcessStub("process5", 9095);
		
		List<NodeInterface> nodes = new ArrayList<>();
		nodes.add(p1);
		nodes.add(p2);
		nodes.add(p3);
		nodes.add(p4);
		nodes.add(p5);

		
		// tests
		// test succ, pred, and keys for each node (process)
		nodes.forEach(node -> {
			try {
				String succ = node.getSuccessor().getNodeName();
				String pred = node.getPredecessor().getNodeName();
				
				assertEquals(esucclist.get(node.getNodeName()), succ);						// succ
				assertEquals(epredlist.get(node.getNodeName()), pred);						// pred
				
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		});
	}

}
