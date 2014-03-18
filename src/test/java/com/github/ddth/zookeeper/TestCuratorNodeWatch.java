package com.github.ddth.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;

public class TestCuratorNodeWatch {

    public static void main(String[] args) throws Exception {
        CuratorFramework framework = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181").retryPolicy(new RetryNTimes(3, 2000)).build();
        try {
            framework.start();
            final NodeCache nodeCache = new NodeCache(framework, "/test");
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override
                public void nodeChanged() throws Exception {
                    ChildData data = nodeCache.getCurrentData();
                    System.out.println(new String(data.getData()) + " / " + data);
                }
            });
            nodeCache.start();

            Thread.sleep(30000);

            nodeCache.close();
        } finally {
            framework.close();
        }
    }
}
