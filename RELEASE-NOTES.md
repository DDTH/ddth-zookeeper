ddth-zookeeper release notes
==========================

0.3.0 - 2014-03-17
------------------
- Merged with [ddth-osgizookeeper](https://github.com/DDTH/ddth-osgizookeeper) and packaged as OSGi bundle.


0.2.0 - 2014-03-05
------------------
- Use [Apache Curator](http://curator.apache.org/index.html) as the underlying ZooKeeper client library.
- New methods: `ZooKeeperClient.curatorFramework()`, `String[] ZooKeeperClient.getChildren(String)`.


0.1.1 - 2014-02-17
------------------
- First release.
