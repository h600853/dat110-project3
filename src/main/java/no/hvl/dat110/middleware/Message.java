package no.hvl.dat110.middleware;

import java.io.Serializable;
import java.math.BigInteger;
import java.rmi.RemoteException;

public class Message implements Serializable {
	

	private static final long serialVersionUID = 1L;
	private int clock = 0;
	private BigInteger nodeID;
	private String nodename;
	private int port;
	
	private boolean acknowledged = false;
	private String filepath;
	
	private byte[] bytesOfFile; 						// we use bytes here to indicate that we can actually divide the file into byte chunks and send them to different servers
	private BigInteger hashOfFile;
	private String nameOfFile;
	
	/** variable for remote-write protocol*/
	private boolean primaryServer;
	
	
	public Message() throws RemoteException {
		super();
	}
	
	public Message(BigInteger nodeID, String nodename, int port) {
		this.nodeID = nodeID;
		this.nodename = nodename;
		this.port = port;
	}
	
	public int getClock() {
		return clock;
	}
	
	public void setClock(int clock) {
		this.clock = clock;
	}

	public BigInteger getNodeID() {
		return nodeID;
	}

	public void setNodeID(BigInteger nodeID) {
		this.nodeID = nodeID;
	}

	public boolean isAcknowledged() {
		return acknowledged;
	}

	public void setAcknowledged(boolean acknowledged) {
		this.acknowledged = acknowledged;
	}

	public String getNodeName() {
		return nodename;
	}

	public void setNodeName(String nodename) {
		this.nodename = nodename;
	}

	public String getFilepath() {
		return filepath;
	}

	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}

	/**
	 * @return the bytesOfFile
	 */
	public byte[] getBytesOfFile() {
		return bytesOfFile;
	}

	/**
	 * @param bytesOfFile the bytesOfFile to set
	 */
	public void setBytesOfFile(byte[] bytesOfFile) {
		this.bytesOfFile = bytesOfFile;
	}

	/**
	 * @return the hashOfFile
	 */
	public BigInteger getHashOfFile() {
		return hashOfFile;
	}

	/**
	 * @param hashOfFile the hashOfFile to set
	 */
	public void setHashOfFile(BigInteger hashOfFile) {
		this.hashOfFile = hashOfFile;
	}

	/**
	 * @return the nameOfFile
	 */
	public String getNameOfFile() {
		return nameOfFile;
	}

	/**
	 * @param nameOfFile the nameOfFile to set
	 */
	public void setNameOfFile(String nameOfFile) {
		this.nameOfFile = nameOfFile;
	}

	/**
	 * @return the port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * @param port the port to set
	 */
	public void setPort(int port) {
		this.port = port;
	}

	/**
	 * @return the primaryServer
	 */
	public boolean isPrimaryServer() {
		return primaryServer;
	}

	/**
	 * @param primaryServer the primaryServer to set
	 */
	public void setPrimaryServer(boolean primaryServer) {
		this.primaryServer = primaryServer;
	}
	
}
