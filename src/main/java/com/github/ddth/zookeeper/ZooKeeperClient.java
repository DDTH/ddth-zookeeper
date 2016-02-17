package com.github.ddth.zookeeper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseDao;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A simple ZooKeeper client.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class ZooKeeperClient extends BaseDao implements Watcher, BackgroundCallback {

    private final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClient.class);

    /**
     * Default session timeout (30 seconds, in milliseconds)
     */
    public final static int DEFAULT_SESSION_TIMEOUT = 30000;

    private String connectString;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    private LoadingCache<String, NodeCache> cacheNodeWatcher;

    private String cacheNameRaw, cacheNameJson;

    /**
     * @since 0.2.0
     */
    private CuratorFramework curatorFramework;

    /**
     * Constructs a new {@link ZooKeeperClient} instance.
     */
    public ZooKeeperClient() {
    }

    /**
     * Constructs a new {@link ZooKeeperClient} instance.
     * 
     * @param connectString
     */
    public ZooKeeperClient(String connectString) {
        this.connectString = connectString;
    }

    /**
     * Constructs a new {@link ZooKeeperClient} instance.
     * 
     * @param connectString
     * @param sessionTimeoutMillisec
     */
    public ZooKeeperClient(String connectString, int sessionTimeoutMillisec) {
        this.connectString = connectString;
        this.sessionTimeout = sessionTimeoutMillisec;
    }

    /**
     * Gets ZooKeeper connection string.
     * 
     * @return
     * @since 0.4.0
     */
    public String getConnectString() {
        return connectString;
    }

    /**
     * Sets ZooKeeper connection string.
     * 
     * @param connectString
     * @return
     * @since 0.4.0
     */
    public ZooKeeperClient setConnectString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * Gets ZooKeeper connection string.
     * 
     * @return
     * @deprecated since 0.4.0, use {@link #getConnectString()}
     */
    @Deprecated
    public String connectionString() {
        return connectString;
    }

    /**
     * Sets ZooKeeper connection string.
     * 
     * @param connectString
     * @return
     * @deprecated since 0.4.0, use {@link #setConnectString(String)}
     */
    @Deprecated
    public ZooKeeperClient conectionString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * Gets ZooKeeper session timeout (in milliseconds).
     * 
     * @return
     * @since 0.4.0
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets ZooKeeper session timeout (in milliseconds).
     * 
     * @param sessionTimeoutMs
     * @return
     * @since 0.4.0
     */
    public ZooKeeperClient setSessionTimeout(int sessionTimeoutMs) {
        this.sessionTimeout = sessionTimeoutMs;
        return this;
    }

    /**
     * Gets ZooKeeper session timeout (in milliseconds).
     * 
     * @return
     * @deprecated since 0.4.0, use {@link #getSessionTimeout()}
     */
    @Deprecated
    public int sessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets ZooKeeper session timeout in milliseconds.
     * 
     * @param sessionTimeoutMillisec
     * @return
     * @deprecated since 0.4.0, use {@link #setSessionTimeout(int)}
     */
    @Deprecated
    public ZooKeeperClient sessionTimeout(int sessionTimeoutMillisec) {
        this.sessionTimeout = sessionTimeoutMillisec;
        return this;
    }

    /**
     * Gets name of cache to store raw data fetched from ZooKeeper.
     * 
     * @return
     * @since 0.4.0
     */
    public String getCacheNameRaw() {
        return cacheNameRaw;
    }

    /**
     * Sets name of cache to store raw data fetched from ZooKeeper.
     * 
     * @param cacheNameRaw
     * @return
     * @since 0.4.0
     */
    public ZooKeeperClient setCacheNameRaw(String cacheNameRaw) {
        this.cacheNameRaw = cacheNameRaw;
        return this;
    }

    /**
     * Gets name of cache to store Json data fetched from ZooKeeper.
     * 
     * @return
     * @since 0.4.0
     */
    public String getCacheNameJson() {
        return cacheNameJson;
    }

    /**
     * Sets name of cache to store Json data fetched from ZooKeeper.
     * 
     * @param cacheNameJson
     * @return
     * @since 0.4.0
     */
    public ZooKeeperClient setCacheNameJson(String cacheNameJson) {
        this.cacheNameJson = cacheNameJson;
        return this;
    }

    /**
     * Gets the underlying {@link CuratorFramework}.
     * 
     * @return
     * @since 0.4.0
     */
    public CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }

    /**
     * Gets the underlying {@link CuratorFramework}.
     * 
     * @return
     * @since 0.2.0
     * @deprecated since 0.4.0, use {@link #getCuratorFramework()}
     */
    @Deprecated
    public CuratorFramework curatorFramework() {
        return curatorFramework;
    }

    // private final static List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    /**
     * Creates a new node, with initial data.
     * 
     * @param path
     * @param data
     * @param createMode
     * @return
     * @throws ZooKeeperException
     */
    private boolean _create(String path, byte[] data, CreateMode createMode)
            throws ZooKeeperException {
        if (data == null) {
            data = ArrayUtils.EMPTY_BYTE_ARRAY;
        }
        try {
            curatorFramework.create().creatingParentsIfNeeded().withMode(createMode)
                    .forPath(path, data);
            _invalidateCache(path);
            return true;
        } catch (InterruptedException e) {
            return false;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        } catch (KeeperException.ConnectionLossException e) {
            throw new ZooKeeperException.ClientDisconnectedException();
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Watches a node for changes.
     * 
     * @param path
     * @throws ZooKeeperException
     */
    private void _watchNode(String path) throws ZooKeeperException {
        try {
            cacheNodeWatcher.get(path);
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Reads raw data from a node.
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    private byte[] _readRaw(String path) throws ZooKeeperException {
        try {
            byte[] data = curatorFramework.getData().forPath(path);
            // if (cacheRaw != null) {
            _watchNode(path);
            // }
            return data;
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (KeeperException.ConnectionLossException e) {
            throw new ZooKeeperException.ClientDisconnectedException();
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Reads Json data from a node.
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    private Object _readJson(String path) throws ZooKeeperException {
        String jsonString = getData(path);
        try {
            return jsonString != null ? SerializationUtils.fromJsonString(jsonString) : null;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    private boolean _write(String path, byte[] data, boolean createNodes) throws ZooKeeperException {
        try {
            boolean result = true;
            if (createNodes && !nodeExists(path)) {
                result = _create(path, data, CreateMode.PERSISTENT);
            } else {
                curatorFramework.setData().forPath(path, data);
            }
            if (result) {
                _invalidateCache(path, data);
            }
            return result;
        } catch (InterruptedException e) {
            return false;
        } catch (KeeperException.NoNodeException e) {
            return false;
        } catch (KeeperException.ConnectionLossException e) {
            throw new ZooKeeperException.ClientDisconnectedException();
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    private final static Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Creates an empty ephemeral node.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @return
     * @since 0.4.1
     * @throws ZooKeeperException
     */
    public boolean createEphemeralNode(String path) throws ZooKeeperException {
        return _create(path, null, CreateMode.EPHEMERAL);
    }

    /**
     * Creates an ephemeral node, with initial values.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @since 0.4.1
     * @throws ZooKeeperException
     */
    public boolean createEphemeralNode(String path, byte[] value) throws ZooKeeperException {
        return _create(path, value, CreateMode.EPHEMERAL);
    }

    /**
     * Creates an ephemeral node, with initial values.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @since 0.4.1
     * @throws ZooKeeperException
     */
    public boolean createEphemeralNode(String path, String value) throws ZooKeeperException {
        return _create(path, value != null ? value.getBytes(UTF8) : null, CreateMode.EPHEMERAL);
    }

    /**
     * Creates an empty node.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path) throws ZooKeeperException {
        return _create(path, null, CreateMode.PERSISTENT);
    }

    /**
     * Creates a node, with initial values.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path, byte[] value) throws ZooKeeperException {
        return _create(path, value, CreateMode.PERSISTENT);
    }

    /**
     * Creates a node, with initial values.
     * 
     * <p>
     * Note: nodes are created recursively (parent nodes are created if needed).
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path, String value) throws ZooKeeperException {
        return _create(path, value != null ? value.getBytes(UTF8) : null, CreateMode.PERSISTENT);
    }

    /**
     * Checks if a path exists.
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    public boolean nodeExists(String path) throws ZooKeeperException {
        try {
            Stat stat = curatorFramework.checkExists().forPath(path);
            return stat != null;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Gets children of a node.
     * 
     * @param path
     * @return
     * @since 0.2.0
     * @throws ZooKeeperException
     */
    public String[] getChildren(String path) throws ZooKeeperException {
        try {
            List<String> result = curatorFramework.getChildren().forPath(path);
            return result != null ? result.toArray(ArrayUtils.EMPTY_STRING_ARRAY) : null;
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Reads raw data from a node.
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    public byte[] getDataRaw(String path) throws ZooKeeperException {
        try {
            byte[] data = getFromCache(cacheNameRaw, path, byte[].class);
            if (data == null) {
                data = _readRaw(path);
                putToCache(cacheNameRaw, path, data);
            }
            return data;
        } catch (ZooKeeperException.NodeNotFoundException e) {
            return null;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Reads data from a node as a JSON object.
     * 
     * @param path
     * @return
     * @since 0.3.1
     * @throws ZooKeeperException
     */
    public Object getDataJson(String path) throws ZooKeeperException {
        try {
            Object data = getFromCache(cacheNameJson, path);
            if (data == null) {
                data = _readJson(path);
                putToCache(cacheNameJson, path, data);
            }
            return data;
        } catch (ZooKeeperException.NodeNotFoundException e) {
            return null;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
    }

    /**
     * Reads data from a node.
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    public String getData(String path) throws ZooKeeperException {
        byte[] data = getDataRaw(path);
        return data != null ? new String(data, UTF8) : null;
    }

    /**
     * Removes an existing node.
     * 
     * <p>
     * Node with children will not be removed.
     * </p>
     * 
     * @param path
     * @return {@code true} if node has been removed successfully, {@code false}
     *         otherwise (maybe node is not empty)
     * @since 0.4.1
     * @throws ZooKeeperException
     */
    public boolean removeNode(String path) throws ZooKeeperException {
        return removeNode(path, false);
    }

    /**
     * Removes an existing node.
     * 
     * @param path
     * @param removeChildren
     *            {@code true} to indicate that child nodes should be removed
     *            too
     * @return {@code true} if node has been removed successfully, {@code false}
     *         otherwise (maybe node is not empty)
     * @since 0.4.1
     * @throws ZooKeeperException
     */
    public boolean removeNode(String path, boolean removeChildren) throws ZooKeeperException {
        try {
            if (removeChildren) {
                curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
            } else {
                curatorFramework.delete().forPath(path);
            }
        } catch (KeeperException.NotEmptyException e) {
            return false;
        } catch (KeeperException.NoNodeException e) {
            return true;
        } catch (Exception e) {
            if (e instanceof ZooKeeperException) {
                throw (ZooKeeperException) e;
            } else {
                throw new ZooKeeperException(e);
            }
        }
        _invalidateCache(path);
        return true;
    }

    /**
     * Writes raw data to a node.
     * 
     * @param path
     * @param value
     * @return {@code true} if write successfully, {@code false} otherwise (note
     *         does not exist, for example)
     * @throws ZooKeeperException
     */
    public boolean setData(String path, byte[] value) throws ZooKeeperException {
        return setData(path, value, false);
    }

    /**
     * Writes raw data to a node.
     * 
     * @param path
     * @param value
     * @param createNodes
     *            {@code true} to have nodes to be created if not exist
     * @return {@code true} if write successfully, {@code false} otherwise (note
     *         does not exist, for example)
     * @throws ZooKeeperException
     */
    public boolean setData(String path, byte[] value, boolean createNodes)
            throws ZooKeeperException {
        return _write(path, value, createNodes);
    }

    /**
     * Writes data to a node.
     * 
     * @param path
     * @param value
     * @return {@code true} if write successfully, {@code false} otherwise (note
     *         does not exist, for example)
     * @throws ZooKeeperException
     */
    public boolean setData(String path, String value) throws ZooKeeperException {
        return setData(path, value, false);
    }

    /**
     * Writes data to a node.
     * 
     * @param path
     * @param value
     * @param createNodes
     *            {@code true} to have nodes to be created if not exist
     * @return {@code true} if write successfully, {@code false} otherwise (note
     *         does not exist, for example)
     * @throws ZooKeeperException
     */
    public boolean setData(String path, String value, boolean createNodes)
            throws ZooKeeperException {
        return _write(path, value != null ? value.getBytes(UTF8) : null, createNodes);
    }

    private void _invalidateCache(String path) {
        _invalidateCache(path, null);
    }

    private void _invalidateCache(String path, byte[] newData) {
        ICache cacheJson = cacheNameJson != null ? getCache(cacheNameJson) : null;
        if (cacheJson != null) {
            cacheJson.delete(path);
        }

        ICache cacheRaw = cacheNameRaw != null ? getCache(cacheNameRaw) : null;
        if (cacheRaw != null) {
            if (newData == null) {
                cacheRaw.delete(path);
            } else {
                cacheRaw.set(path, newData);
            }
        }
    }

    /**
     * Connects to ZooKeeper server.
     * 
     * @throws IOException
     */
    private void _connect() throws IOException {
        curatorFramework = CuratorFrameworkFactory.newClient(connectString, sessionTimeout, 5000,
                new RetryNTimes(3, 2000));
        curatorFramework.start();
    }

    /**
     * Disconnects from ZooKeeper server
     * 
     * @throws InterruptedException
     */
    private void _close() throws InterruptedException {
        try {
            if (curatorFramework != null) {
                curatorFramework.close();
            }
        } finally {
            curatorFramework = null;
        }
    }

    /**
     * Re-connects to ZooKeeper server.
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    private void _reconnect() throws InterruptedException, IOException {
        _close();
        _connect();
    }

    private void _destroyNodeWatcher() {
        if (cacheNodeWatcher != null) {
            try {
                cacheNodeWatcher.invalidateAll();
            } finally {
                cacheNodeWatcher = null;
            }
        }
    }

    private void _initCacheWatcher() {
        cacheNodeWatcher = CacheBuilder.newBuilder()
                .concurrencyLevel(Runtime.getRuntime().availableProcessors()).maximumSize(10000)
                .expireAfterAccess(3600, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<String, NodeCache>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, NodeCache> event) {
                        try {
                            event.getValue().close();
                        } catch (IOException e) {
                            LOGGER.warn(e.getMessage(), e);
                        }
                    }
                }).build(new CacheLoader<String, NodeCache>() {
                    @Override
                    public NodeCache load(final String path) throws Exception {
                        final NodeCache nodeCache = new NodeCache(curatorFramework, path);
                        nodeCache.getListenable().addListener(new NodeCacheListener() {
                            @Override
                            public void nodeChanged() throws Exception {
                                ChildData data = nodeCache.getCurrentData();
                                _invalidateCache(path, data != null ? data.getData() : null);
                            }
                        });
                        nodeCache.start();
                        return nodeCache;
                    }
                });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ZooKeeperClient init() {
        super.init();

        try {
            _connect();
            _initCacheWatcher();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            _destroyNodeWatcher();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        try {
            _close();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

        super.destroy();
    }

    private void _eventNodeChanged(WatchedEvent event) throws ZooKeeperException {
        Watcher.Event.EventType type = event.getType();
        String path = event.getPath();
        switch (type) {
        case NodeDataChanged: {
            // reload node data
            byte[] data = _readRaw(path);
            _invalidateCache(path, data);
            break;
        }
        case NodeDeleted: {
            // remove node from cache
            _invalidateCache(path);
            break;
        }
        default: {
            break;
        }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(WatchedEvent event) {
        Watcher.Event.KeeperState state = event.getState();
        try {
            switch (state) {
            case Expired: {
                _reconnect();
                break;
            }
            case SyncConnected: {
                _eventNodeChanged(event);
                break;
            }
            default: {
                LOGGER.debug(event.toString());
            }
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processResult(CuratorFramework curatorFramework, CuratorEvent event)
            throws Exception {
        CuratorEventType eventType = event.getType();
        switch (eventType) {
        case CHILDREN:
            break;
        case CLOSING:
            break;
        case CREATE:
            _invalidateCache(event.getPath());
            break;
        case DELETE:
            _invalidateCache(event.getPath());
            break;
        case EXISTS:
            break;
        case GET_ACL:
            break;
        case GET_DATA:
            break;
        case SET_ACL:
            break;
        case SET_DATA:
            break;
        case SYNC:
            break;
        case WATCHED:
            process(event.getWatchedEvent());
            break;
        default:
            LOGGER.debug(event.toString());
            break;
        }
    }
}
