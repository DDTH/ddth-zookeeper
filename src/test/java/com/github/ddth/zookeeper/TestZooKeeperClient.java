package com.github.ddth.zookeeper;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;

public class TestZooKeeperClient extends TestCase {

    public static Test suite() {
        return new TestSuite(TestZooKeeperClient.class);
    }

    private TestingServer zkServer;

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServer();
    }

    @After
    public void tearDown() throws IOException {
        zkServer.close();
    }

    @org.junit.Test
    public void testConnection() throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient(zkServer.getConnectString());
        try {
            zkClient.init();
        } finally {
            zkClient.destroy();
        }
    }

    @org.junit.Test
    public void testCreateAndExist() throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient(zkServer.getConnectString());
        try {
            zkClient.init();
            assertFalse(zkClient.nodeExists("/test"));
            assertTrue(zkClient.createNode("/test"));
            assertTrue(zkClient.nodeExists("/test"));
        } finally {
            zkClient.destroy();
        }
    }

    @org.junit.Test
    public void testSetGetDataNoAutoCreate() throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient(zkServer.getConnectString());
        try {
            zkClient.init();
            String path = "/demo/parent/child1";
            assertFalse(zkClient.setData(path, "demo"));
            assertNull(zkClient.getData(path));
        } finally {
            zkClient.destroy();
        }
    }

    @org.junit.Test
    public void testSetGetDataAutoCreate() throws Exception {
        ZooKeeperClient zkClient = new ZooKeeperClient(zkServer.getConnectString());
        try {
            zkClient.init();
            String path = "/demo/parent/child1";
            assertFalse(zkClient.setData(path, "demo"));
            assertNull(zkClient.getData(path));
            assertTrue(zkClient.setData(path, "demo", true));
            assertEquals("demo", zkClient.getData(path));
        } finally {
            zkClient.destroy();
        }
    }

    public static void main(String[] args) throws Exception {
        TestingServer zkServer = new TestingServer();
        try {
            ZooKeeperClient zkClient = new ZooKeeperClient(zkServer.getConnectString());
            try {
                zkClient.init();
                Object value = zkClient.curatorFramework().checkExists().forPath("/1/2/3/4/5");
                System.out.println(value);

                value = zkClient.getChildren("/1");
                System.out.println(value);
            } finally {
                zkClient.destroy();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            zkServer.close();
        }
    }

}
