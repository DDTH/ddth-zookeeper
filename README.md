ddth-zookeeper
==============

DDTH's ZooKeeper Libraries and Utilities: simplify ZooKeeper's usage.

Project home:
[https://github.com/DDTH/ddth-zookeeper](https://github.com/DDTH/ddth-zookeeper)

For OSGi environment, see [ddth-osgizookeeper](https://github.com/DDTH/ddth-osgizookeeper).


## License ##

See LICENSE.txt for details. Copyright (c) 2014 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `0.2.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-zookeeper</artifactId>
	<version>0.2.0</version>
</dependency>
```


## Usage ##

Simple usage:

```java
// create and initialize a new ZooKeeperClient client
ZooKeeperClient zkClient = new ZooKeeperClient("localhost:2181");
zkClient.init(); //don't forget to initialize the client

String path;
boolean status;
String data;

// create a new node, recursively
path = "/parent/child/grandchild1";
status = zkClient.createNode(path); //false is returned if node already exists

// create a new node, with initial data
path = "/parent/child/grandchild2";
data = "grandchild2 - content";
status = zkClient.createNode(path, data);

// read node's data
path = "/parent/child/grandchild2";
data = zkClient.getData(path); //returns  "grandchild2 - content";

// write data to node, fails if node does not exist!
path = "/parent/child/grandchild1";
zkClient.setData(path, "demo");
data = zkClient.getData(path); //returns "demo"

// write data to node, node is created if not exists
path = "/parent/child/grandchild3";
data = "grandchild3 - content";
zkClient.setData(path, data, true);
data = zkClient.getData(path); //returns "grandchild3 - content"

// destroy ZooKeeperClient object when finish
zkClient.destroy();
```

Working with [Apache Curator](http://curator.apache.org/index.html) instance:

```java
...
CuratorFramework framework = zkClient.curatorFramework();
framework.create().forPath("/parent/child", new byte[0]);
...
```