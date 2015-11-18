package com.github.ddth.zookeeper.qnd;

import com.github.ddth.zookeeper.ZooKeeperClient;

public class QndEphemeralNode {

    public static void main(String[] args) throws Exception {
        ZooKeeperClient client = new ZooKeeperClient("localhost:2181");
        try {
            client.setSessionTimeout(1000);
            client.init();

            String path = "/tmp/a/b/c/d";
            System.out.println("Create ephemeral node [" + path + "]: "
                    + client.createEphemeralNode(path, "demo"));
        } finally {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);
            }
            // client.destroy();
        }
    }

}
