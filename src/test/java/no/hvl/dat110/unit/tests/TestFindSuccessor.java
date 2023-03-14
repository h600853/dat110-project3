package no.hvl.dat110.unit.tests;



import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.rmi.RemoteException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

class TestFindSuccessor {
	
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
				
		// retrieve the process stubs to be contacted to resolve a key
		NodeInterface p1 = Util.getProcessStub("process1", 9091);
		
		BigInteger key1 = Hash.hashOf("file10");		// 73806995735690189889297542544385123161  | succ=process5 (121411138451101288395601026024677976156)
		BigInteger key2 = Hash.hashOf("file20");		// 127615430233524719490345743798572761786 | succ=process4 (210821560651360572675896360671414673172)
		BigInteger key3 = Hash.hashOf("file31"); 		// 83550242532527638904138483233262313637  | succ=process5 (121411138451101288395601026024677976156)
		
		// expected
		BigInteger key1expected = new BigInteger("121411138451101288395601026024677976156");
		BigInteger key2expected = new BigInteger("210821560651360572675896360671414673172");
		BigInteger key3expected = new BigInteger("121411138451101288395601026024677976156");
		
		BigInteger key1actual = p1.findSuccessor(key1).getNodeID();
		BigInteger key2actual = p1.findSuccessor(key2).getNodeID();
		BigInteger key3actual = p1.findSuccessor(key3).getNodeID();
		
		assertEquals(key1expected, key1actual);
		assertEquals(key2expected, key2actual);
		assertEquals(key3expected, key3actual);
		
	}

}
