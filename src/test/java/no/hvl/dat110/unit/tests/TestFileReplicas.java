package no.hvl.dat110.unit.tests;



import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import no.hvl.dat110.util.FileManager;
import no.hvl.dat110.util.Util;

class TestFileReplicas {


	@Test
	void test() throws InterruptedException, RemoteException {
		
		FileManager fm = new FileManager(null, Util.numReplicas);
		
		List<BigInteger> file1expected = new ArrayList<>();
		// filename = file1
		file1expected.add(new BigInteger("73806995735690189889297542544385123161"));
		file1expected.add(new BigInteger("140065589670395970233963560630295105848"));
		file1expected.add(new BigInteger("13988058880685873568223126745537177142"));
		file1expected.add(new BigInteger("22851974182570490653634187770374799407"));
		
		fm.setFilename("file1");
		fm.createReplicaFiles();
		BigInteger[] file1actual = fm.getReplicafiles();
		
		List<BigInteger> file1actual_list = Arrays.asList(file1actual);
		
		Collections.sort(file1expected);
		Collections.sort(file1actual_list);
		
		assertTrue(file1expected.equals(file1actual_list));
		
	}

}
