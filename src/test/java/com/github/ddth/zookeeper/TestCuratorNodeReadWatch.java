package com.github.ddth.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class TestCuratorNodeReadWatch {

    public static void main(String[] args) throws Exception {
        CuratorFramework framework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181").retryPolicy(new RetryNTimes(3, 2000)).build();
        try {
            framework.start();
            byte[] data = framework.getData().watched().forPath("/test");
            // .`.inBackground(new BackgroundCallback() {
            // @Override
            // public void processResult(CuratorFramework client, CuratorEvent
            // event)
            // throws Exception {
            // System.out.println(event);
            // }
            // }).forPath("/test");
            System.out.println(new String(data));

            Thread.sleep(30000);
        } finally {
            framework.close();
        }
    }
}
