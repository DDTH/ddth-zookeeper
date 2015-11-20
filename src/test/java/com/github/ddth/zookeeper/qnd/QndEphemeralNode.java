package com.github.ddth.zookeeper.qnd;

import com.github.ddth.zookeeper.ZooKeeperClient;

public class QndEphemeralNode {

    public static void main(String[] args) throws Exception {
        ZooKeeperClient client = new ZooKeeperClient("localhost:2181");
        try {
            client.setSessionTimeout(1000);
            client.init();

            System.out.println("Delete node [/tmp]: " + client.removeNode("/tmp", true));

            String path = "/nodes/connect/192.168.1.10";
            System.out.println("Create ephemeral node [" + path + "]: "
                    + client.createEphemeralNode(path, "demo"));
        } finally {
            // for (int i = 0; i < 10; i++) {
            // Thread.sleep(1000);
            // }
            client.destroy();
        }
    }

}
