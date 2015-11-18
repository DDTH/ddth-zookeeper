package com.github.ddth.zookeeper.qnd;

import com.github.ddth.zookeeper.ZooKeeperClient;

public class QndCreateDeleteNode {

    public static void main(String[] args) {
        ZooKeeperClient client = new ZooKeeperClient("localhost:2181");
        try {
            client.init();

            String path = "/tmp/a/b/c/d";
            System.out.println("Create node [" + path + "]: " + client.createNode(path, "demo"));
            System.out.println("Create node [" + path + "]: " + client.createNode(path, "demo"));

            path = "/tmp";
            System.out.println("Delete node [" + path + "]: " + client.removeNode(path));
            System.out.println("Delete node [" + path + "]: " + client.removeNode(path, true));
        } finally {
            client.destroy();
        }
    }

}
