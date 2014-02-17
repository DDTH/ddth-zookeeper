package com.github.ddth.zookeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;

public class TestZooKeeperClient extends TestCase {

	public static Test suite() {
		return new TestSuite(TestZooKeeperClient.class);
	}

	final static int clientPort = 21818;
	final static int numConnections = 1000;
	final static int tickTime = 2000;
	final static Random random = new Random(System.currentTimeMillis());
	final static String dataDirectory = System.getProperty("java.io.tmpdir");

	private ServerCnxnFactory standaloneServerFactory;

	@Before
	public void setUp() throws IOException, InterruptedException {
		File dir = new File(dataDirectory, "zookeeper_"
				+ random.nextInt((int) (System.currentTimeMillis() / 1000)))
				.getAbsoluteFile();
		ZooKeeperServer server = new ZooKeeperServer(dir, dir, tickTime);
		standaloneServerFactory = ServerCnxnFactory.createFactory(
				new InetSocketAddress(clientPort), numConnections);
		standaloneServerFactory.startup(server); // start the server.
	}

	@After
	public void tearDown() {
		standaloneServerFactory.shutdown();
	}

	@org.junit.Test
	public void testConnection() throws Exception {
		ZooKeeperClient zkClient = new ZooKeeperClient("localhost:"
				+ clientPort);
		try {
			zkClient.init();
		} finally {
			zkClient.destroy();
		}
	}

	@org.junit.Test
	public void testCreate() throws Exception {
		ZooKeeperClient zkClient = new ZooKeeperClient("localhost:"
				+ clientPort);
		try {
			zkClient.init();
			assertTrue(zkClient.createNode("/test"));
			assertTrue(zkClient.nodeExists("/test"));
		} finally {
			zkClient.destroy();
		}
	}

	@org.junit.Test
	public void testExists() throws Exception {
		ZooKeeperClient zkClient = new ZooKeeperClient("localhost:"
				+ clientPort);
		try {
			zkClient.init();
			assertFalse(zkClient.nodeExists("/test"));
		} finally {
			zkClient.destroy();
		}
	}

	@org.junit.Test
	public void testSetGetData() throws Exception {
		ZooKeeperClient zkClient = new ZooKeeperClient("localhost:"
				+ clientPort);
		try {
			zkClient.init();
			String path = "/demo/node1";
			assertTrue(zkClient.setData(path, "demo", true));
			assertEquals("demo", zkClient.getData(path));
		} finally {
			zkClient.destroy();
		}
	}

	public static void main(String[] args) throws Exception {
		ZooKeeperClient zkClient = new ZooKeeperClient("localhost:2181", 100);
		try {
			zkClient.init();
			String path = "/1/2/3/4/5";
			zkClient.createNode(path, "demo");
			Thread.sleep(15000);
			System.out.println(zkClient.getData(path));
			Thread.sleep(15000);
			System.out.println(zkClient.getData(path));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			zkClient.destroy();
		}
	}

}
