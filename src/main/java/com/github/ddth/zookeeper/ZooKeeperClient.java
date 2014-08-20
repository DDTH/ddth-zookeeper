package com.github.ddth.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.utils.SerializationUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * A simple ZooKeeper client.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class ZooKeeperClient implements Watcher, BackgroundCallback {

    private final Logger LOGGER = LoggerFactory.getLogger(ZooKeeperClient.class);

    /**
     * Default session timeout (1 hour, in milliseconds)
     */
    public final static int DEFAULT_SESSION_TIMEOUT = 3600000;

    private String connectString;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    private boolean cacheEnabled = true;

    private LoadingCache<String, NodeCache> cacheNodeWatcher;

    private LoadingCache<String, byte[]> cacheRaw;
    private LoadingCache<String, Object> cacheJson;

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
     */
    public String connectionString() {
        return connectString;
    }

    /**
     * Sets ZooKeeper connection string.
     * 
     * @param connectString
     * @return
     */
    public ZooKeeperClient conectionString(String connectString) {
        this.connectString = connectString;
        return this;
    }

    /**
     * Gets ZooKeeper session timeout (in milliseconds).
     * 
     * @return
     */
    public int sessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets ZooKeeper session timeout in milliseconds.
     * 
     * @param sessionTimeoutMillisec
     * @return
     */
    public ZooKeeperClient sessionTimeout(int sessionTimeoutMillisec) {
        this.sessionTimeout = sessionTimeoutMillisec;
        return this;
    }

    /**
     * Is caching enabled?
     * 
     * @return
     */
    public boolean cacheEnabled() {
        return cacheEnabled;
    }

    /**
     * Enables/Disables caching.
     * 
     * @param cacheEnabled
     * @return
     */
    public ZooKeeperClient cacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
        return this;
    }

    /**
     * Gets the underlying {@link CuratorFramework}.
     * 
     * @return
     * @since 0.2.0
     */
    public CuratorFramework curatorFramework() {
        return curatorFramework;
    }

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final static String[] EMPTY_STRING_ARRAY = new String[0];

    // private final static List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private boolean _create(String path, byte[] data) throws ZooKeeperException {
        if (data == null) {
            data = EMPTY_BYTE_ARRAY;
        }
        try {
            String[] tokens = path.replaceAll("^\\/+", "").replaceAll("\\/+$", "").split("\\/");
            StringBuilder _path = new StringBuilder();
            for (int i = 0, n = tokens.length - 1; i < n; i++) {
                _path.append("/").append(tokens[i]);
                try {
                    curatorFramework.create().forPath(_path.toString(), EMPTY_BYTE_ARRAY);
                } catch (KeeperException.NodeExistsException e) {
                    // ignore
                }
            }
            curatorFramework.create().forPath(path, data);

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

    private byte[] _readRaw(String path) throws ZooKeeperException {
        try {
            byte[] data = curatorFramework.getData().forPath(path);
            if (cacheRaw != null) {
                _watchNode(path);
            }
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
                result = _create(path, data);
            } else {
                curatorFramework.setData().forPath(path, data);
            }
            if (cacheRaw != null && result) {
                _invalidateCache();
                cacheRaw.put(path, data);
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

    /**
     * Creates an empty node.
     * 
     * <p>
     * Note: nodes are created recursively.
     * </p>
     * 
     * @param path
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path) throws ZooKeeperException {
        return _create(path, null);
    }

    /**
     * Creates a node.
     * 
     * <p>
     * Note: nodes are created recursively.
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path, byte[] value) throws ZooKeeperException {
        return _create(path, value);
    }

    /**
     * Creates a node.
     * 
     * <p>
     * Note: nodes are created recursively.
     * </p>
     * 
     * @param path
     * @param value
     * @return
     * @throws ZooKeeperException
     */
    public boolean createNode(String path, String value) throws ZooKeeperException {
        try {
            return _create(path, value != null ? value.getBytes("UTF-8") : null);
        } catch (UnsupportedEncodingException e) {
            return false;
        }
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
            return result != null ? result.toArray(EMPTY_STRING_ARRAY) : null;
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
            return cacheRaw != null ? cacheRaw.get(path) : _readRaw(path);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ZooKeeperException.NodeNotFoundException) {
                return null;
            }
            throw new ZooKeeperException(e.getCause());
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof ZooKeeperException.NodeNotFoundException) {
                return null;
            }
            throw new ZooKeeperException(e.getCause());
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
            return cacheJson != null ? cacheJson.get(path) : _readJson(path);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ZooKeeperException.NodeNotFoundException) {
                return null;
            }
            throw new ZooKeeperException(e.getCause());
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof ZooKeeperException.NodeNotFoundException) {
                return null;
            }
            throw new ZooKeeperException(e.getCause());
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
        try {
            return data != null ? new String(data, "UTF-8") : null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
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
        try {
            return _write(path, value != null ? value.getBytes("UTF-8") : null, createNodes);
        } catch (UnsupportedEncodingException e) {
            return false;
        }
    }

    private void _invalidateCache() {
        if (cacheRaw != null) {
            cacheRaw.invalidateAll();
        }

        if (cacheJson != null) {
            cacheJson.invalidateAll();
        }
    }

    private void _invalidateCache(String path) {
        if (cacheRaw != null) {
            cacheRaw.invalidate(path);
        }

        if (cacheJson != null) {
            cacheJson.invalidate(path);
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
        _invalidateCache();
    }

    /**
     * Disconnects rom ZooKeeper server
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

    private void _destroyCache() {
        if (cacheRaw != null) {
            cacheRaw.invalidateAll();
            cacheRaw = null;
        }

        if (cacheJson != null) {
            cacheJson.invalidateAll();
            cacheJson = null;
        }
    }

    private void _initCache() {
        if (cacheEnabled) {
            cacheRaw = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(Integer.MAX_VALUE).expireAfterAccess(3600, TimeUnit.SECONDS)
                    .build(new CacheLoader<String, byte[]>() {
                        @Override
                        public byte[] load(String path) throws Exception {
                            byte[] result = _readRaw(path);
                            if (result == null) {
                                throw new ZooKeeperException.NodeNotFoundException();
                            }
                            return result;
                        }
                    });

            cacheJson = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(Integer.MAX_VALUE).expireAfterAccess(3600, TimeUnit.SECONDS)
                    .build(new CacheLoader<String, Object>() {
                        @Override
                        public Object load(String path) throws Exception {
                            Object result = _readJson(path);
                            if (result == null) {
                                throw new ZooKeeperException.NodeNotFoundException();
                            }
                            return result;
                        }
                    });
        }
    }

    private void _destroyNodeWatcher() {
        if (cacheNodeWatcher != null) {
            cacheNodeWatcher.invalidateAll();
            cacheNodeWatcher = null;
        }
    }

    private void _initCacheWatcher() {
        if (cacheEnabled) {
            cacheNodeWatcher = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(Integer.MAX_VALUE).expireAfterAccess(3600, TimeUnit.SECONDS)
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
                                    _invalidateCache(path);
                                    ChildData data = nodeCache.getCurrentData();
                                    if (data != null) {
                                        cacheRaw.put(path, data.getData());
                                    }
                                }
                            });
                            nodeCache.start();
                            return nodeCache;
                        }
                    });
        }
    }

    /**
     * Initializing method.
     * 
     * @throws Exception
     */
    public void init() throws Exception {
        _connect();
        _initCache();
        _initCacheWatcher();
    }

    /**
     * Destroying method.
     * 
     * @throws Exception
     */
    public void destroy() {
        try {
            _destroyCache();
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }

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
    }

    private void _eventNodeChanged(WatchedEvent event) throws ZooKeeperException {
        Watcher.Event.EventType type = event.getType();
        String path = event.getPath();
        switch (type) {
        case NodeDataChanged: {
            // reload node data
            byte[] data = _readRaw(path);
            cacheRaw.put(path, data);
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
            case Disconnected: {
                _invalidateCache();
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

    @Override
    public void processResult(CuratorFramework curatorFramework, CuratorEvent event)
            throws Exception {
        CuratorEventType eventType = event.getType();
        switch (eventType) {
        case CHILDREN:
            break;
        case CLOSING:
            _invalidateCache();
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
