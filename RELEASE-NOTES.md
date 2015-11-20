ddth-zookeeper release notes
============================

0.4.1.1 - 2015-11-20
--------------------

- Bug fix: removing an non-exist node should NOT throw exception.


0.4.1 - 2015-11-18
------------------

- APIs to remove node(s) and create ephemeral node.
- Set default ZooKeeper session timeout to 30 seconds.


0.4.0 - 2015-11-16
------------------

- Use `ddth-cache-adapter` for caching.
- Minor improvements & enhancements.


0.3.1.3 - 2014-08-20
--------------------

- Minor update: `ZooKeeperException` now extends `RuntimeException`.


0.3.1.1 - 2014-03-18
--------------------

- Improve caching.
- New method `Object ZooKeeperClient.getDataJson(String)`.
- Minor POM fix.


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
