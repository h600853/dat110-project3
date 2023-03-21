package no.hvl.dat110.util;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash { 
	
	
	public static BigInteger hashOf(String entity) {	
		// Task: Hash a given string using MD5 and return the result as a BigInteger.

		// we use MD5 with 128 bits digest

		// compute the hash of the input 'entity'

		// convert the hash into hex format

		// convert the hex into BigInteger

		// return the BigInteger

		BigInteger hashint = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] hashBytes = md.digest(entity.getBytes());
			String hashHex = toHex(hashBytes);
			hashint = new BigInteger(hashHex,16);

		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}


		return hashint;
	}
	
	public static BigInteger addressSize() {

		// Task: compute the address size of MD5
		// compute the number of bits = bitSize()
		// compute the address size = 2 ^ number of bits
		// return the address size

		int bits = bitSize();
		double adressSize = Math.pow(2, bits);
		BigDecimal bd = new BigDecimal(adressSize);
		BigInteger answer = bd.toBigInteger();

		return answer;
	}
	
	public static int bitSize() {
		
		int digestlen = 16;
		
		// find the digest length
		
		return digestlen*8;
	}
	
	public static String toHex(byte[] digest) {
		StringBuilder strbuilder = new StringBuilder();
		for(byte b : digest) {
			strbuilder.append(String.format("%02x", b&0xff));
		}
		return strbuilder.toString();
	}

}
