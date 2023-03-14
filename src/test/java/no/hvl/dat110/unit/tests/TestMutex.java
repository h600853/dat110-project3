package no.hvl.dat110.unit.tests;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;

class TestMutex {
	
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
		
		// retrieve the processes stubs
		NodeInterface p1 = Util.getProcessStub("process1", 9091);
		NodeInterface p2 = Util.getProcessStub("process2", 9092);
		NodeInterface p3 = Util.getProcessStub("process3", 9093);
		NodeInterface p4 = Util.getProcessStub("process4", 9094);
		NodeInterface p5 = Util.getProcessStub("process5", 9095);
		
		FileManager fm = new FileManager(p3, Util.numReplicas); 		// we are using p3
		String filename = "file1";
		String newupdate = "overwrite the content of existing file - i.e. file1";
		
		Set<Message> activepeers = fm.requestActiveNodesForFile(filename);  // p1, p2, p4, and p5 are holding file1
		
		p3.multicastReleaseLocks(activepeers); 		// reset all locks before voting for mutual exclusion
		
		// request to update the same file will happen concurrently - only one process should get the vote		
		FileUpdater fu1 = new FileUpdater(p1, getPeerMessage(activepeers, p1.getNodeName()), newupdate.getBytes(), activepeers);
		FileUpdater fu2 = new FileUpdater(p2, getPeerMessage(activepeers, p2.getNodeName()), newupdate.getBytes(), activepeers);
		FileUpdater fu4 = new FileUpdater(p4, getPeerMessage(activepeers, p4.getNodeName()), newupdate.getBytes(), activepeers);
		FileUpdater fu5 = new FileUpdater(p5, getPeerMessage(activepeers, p5.getNodeName()), newupdate.getBytes(), activepeers);
		
		
		fu1.start();
		fu2.start();
		fu4.start();
		fu5.start();
		
		fu1.join();
		fu2.join();
		fu4.join();
		fu5.join();
		
		System.out.println("fu1: "+fu1.getReply());
		System.out.println("fu2: "+fu2.getReply());
		System.out.println("fu4: "+fu4.getReply());
		System.out.println("fu5: "+fu5.getReply());
		
		List<Boolean> replies = new ArrayList<>();
		replies.add(fu1.getReply());
		replies.add(fu2.getReply());
		replies.add(fu4.getReply());
		replies.add(fu5.getReply());
		
		Boolean[] expected = {true, false, false, false};
		List<Boolean> e = Arrays.asList(expected);
		Collections.sort(replies);
		Collections.sort(e);
		
		System.out.println(replies+"|"+e);
		
		assertArrayEquals(e.toArray(), replies.toArray());
	}
	
	class FileUpdater extends Thread {
		
		boolean reply;
		NodeInterface node;
		Message peer;
		byte[] updates;
		Set<Message> activepeers;
		
		FileUpdater(NodeInterface node, Message peer, byte[] updates, Set<Message> activepeers) throws RemoteException{
			this.node = node;
			this.peer = peer;
			this.updates = updates;
			this.activepeers = activepeers;
		}
		
		public void run() {
			try {
				reply = node.requestMutexWriteOperation(peer, updates, activepeers);
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
		
		public boolean getReply() {			
			
			return reply;
		}
	}
	
	private Message getPeerMessage(Set<Message> activenodes, String peer) {
		
		Message pmsg = null;
		Iterator<Message> it = activenodes.iterator();
		while(it.hasNext()) {
			Message n = it.next();
			if(n.getNodeName().equals(peer))
				return n;
		}
		
		return pmsg;
	}

}
