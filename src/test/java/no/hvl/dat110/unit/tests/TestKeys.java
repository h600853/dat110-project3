/**
 * 
 */
package no.hvl.dat110.unit.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigInteger;
import java.rmi.RemoteException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
class TestKeys {

	static Map<String, List<BigInteger>> nodeKeys;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		
		// we use MD5 = 128bits digest 	
		// keys per node - expected
		nodeKeys = new HashMap<>();
		List<BigInteger> keys = new ArrayList<>();
		keys.add(new BigInteger("73806995735690189889297542544385123161"));
		keys.add(new BigInteger("83550242532527638904138483233262313637"));
		keys.add(new BigInteger("83789482216496502817864659214345176161"));
		keys.add(new BigInteger("93727835460693461258854490021087346760"));
		nodeKeys.put("process5", keys);
		
		keys = new ArrayList<>();
		keys.add(new BigInteger("66481538825926898419352978747807121875"));
		nodeKeys.put("process3", keys);
		
		keys = new ArrayList<>();
		keys.add(new BigInteger("127615430233524719490345743798572761786"));
		keys.add(new BigInteger("136541484142652779330200457892076216958"));
		keys.add(new BigInteger("140065589670395970233963560630295105848"));
		keys.add(new BigInteger("149037318589456997023767356092491379363"));
		keys.add(new BigInteger("179241403734895214076219526399432130488"));
		keys.add(new BigInteger("191558309544647931649133602766041063646"));
		nodeKeys.put("process4", keys);
		
		keys = new ArrayList<>();
		keys.add(new BigInteger("22851974182570490653634187770374799407"));
		keys.add(new BigInteger("29249986233499374510233936914584139597"));
		nodeKeys.put("process1", keys);
		
		keys = new ArrayList<>();
		keys.add(new BigInteger("8256520967608282605234844990226290265"));
		keys.add(new BigInteger("13988058880685873568223126745537177142"));
		keys.add(new BigInteger("214974501590503159329658682485012382526"));
		keys.add(new BigInteger("239639416547729385993578111985362970770"));
		keys.add(new BigInteger("240788495190661943955842370400050808545"));
		keys.add(new BigInteger("263856675938514540210526796966916740559"));
		keys.add(new BigInteger("305513342937436802305366564249075562188"));
		nodeKeys.put("process2", keys);
		
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
	void test() throws InterruptedException, NoSuchAlgorithmException, IOException {
		
		// retrieve the processes' stubs
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

		// test keys for each node (process)
		nodes.forEach(node -> {
			try {
				List<BigInteger> keysactual = nodeKeys.get(node.getNodeName());
				Collections.sort(keysactual);
				
				List<BigInteger> keysexpected = toList(node.getNodeKeys());
				Collections.sort(keysexpected);
				
				assertArrayEquals(keysexpected.toArray(), keysactual.toArray());			// keys
				
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		});
	}
	
	private List<BigInteger> toList(Set<BigInteger> list){
		
		List<BigInteger> nlist = new ArrayList<>();
		list.forEach(e -> {
			nlist.add(e);
		});
		
		return nlist;
	}

}
