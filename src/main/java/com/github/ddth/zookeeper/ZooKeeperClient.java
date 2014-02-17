package com.github.ddth.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * A simple ZooKeeper client.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class ZooKeeperClient implements Watcher {

	private final Logger LOGGER = LoggerFactory
			.getLogger(ZooKeeperClient.class);

	/**
	 * Default session timeout (1 hour, in milliseconds)
	 */
	public final static int DEFAULT_SESSION_TIMEOUT = 3600000;

	private String connectString;
	private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;
	private boolean cacheEnabled = true;
	private LoadingCache<String, byte[]> cache;

	private ZooKeeper zk;

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

	private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
	private final static List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

	private boolean _create(String path, byte[] data) throws ZooKeeperException {
		if (data == null) {
			data = EMPTY_BYTE_ARRAY;
		}
		try {
			String[] tokens = path.replaceAll("^\\/+", "")
					.replaceAll("\\/+$", "").split("\\/");
			StringBuilder _path = new StringBuilder();
			for (int i = 0, n = tokens.length - 1; i < n; i++) {
				_path.append("/").append(tokens[i]);
				try {
					zk.create(_path.toString(), EMPTY_BYTE_ARRAY, ACL,
							CreateMode.PERSISTENT);
				} catch (KeeperException.NodeExistsException e) {
					// ignore
				}
			}
			zk.create(path, data, ACL, CreateMode.PERSISTENT);
			return true;
		} catch (InterruptedException e) {
			return false;
		} catch (KeeperException.NodeExistsException e) {
			return false;
		} catch (KeeperException.ConnectionLossException e) {
			throw new ZooKeeperException.ClientDisconnectedException();
		} catch (Exception e) {
			throw new ZooKeeperException(e);
		}
	}

	private byte[] _read(String path) throws ZooKeeperException {
		Stat stat = new Stat();
		try {
			boolean watch = cache != null;
			byte[] data = zk.getData(path, watch, stat);
			return data;
		} catch (KeeperException.NoNodeException e) {
			return null;
		} catch (KeeperException.ConnectionLossException e) {
			throw new ZooKeeperException.ClientDisconnectedException();
		} catch (Exception e) {
			throw new ZooKeeperException(e);
		}
	}

	private boolean _write(String path, byte[] data, boolean createNodes)
			throws ZooKeeperException {
		try {
			boolean result = true;
			if (createNodes && !nodeExists(path)) {
				result = _create(path, data);
			} else {
				zk.setData(path, data, -1);
			}
			if (cache != null && result) {
				cache.put(path, data);
			}
			return result;
		} catch (InterruptedException e) {
			return false;
		} catch (KeeperException.NoNodeException e) {
			return false;
		} catch (KeeperException.ConnectionLossException e) {
			throw new ZooKeeperException.ClientDisconnectedException();
		} catch (Exception e) {
			throw new ZooKeeperException(e);
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
	public boolean createNode(String path, byte[] value)
			throws ZooKeeperException {
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
	public boolean createNode(String path, String value)
			throws ZooKeeperException {
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
			Stat stat = zk.exists(path, false);
			return stat != null;
		} catch (Exception e) {
			throw new ZooKeeperException(e);
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
			return cache != null ? cache.get(path) : _read(path);
		} catch (ExecutionException e) {
			if (e.getCause() instanceof ZooKeeperException.NodeNotFoundException) {
				return null;
			}
			throw new ZooKeeperException(e.getCause());
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
			return _write(path, value != null ? value.getBytes("UTF-8") : null,
					createNodes);
		} catch (UnsupportedEncodingException e) {
			return false;
		}
	}

	private void _invalidateCache() {
		if (cache != null) {
			cache.invalidateAll();
		}
	}

	private void _invalidateCache(String path) {
		if (cache != null) {
			cache.invalidate(path);
		}
	}

	/**
	 * Connects to ZooKeeper server.
	 * 
	 * @throws IOException
	 */
	private void _connect() throws IOException {
		zk = new ZooKeeper(connectString, sessionTimeout, this);
		_invalidateCache();
	}

	/**
	 * Disconnects rom ZooKeeper server
	 * 
	 * @throws InterruptedException
	 */
	private void _close() throws InterruptedException {
		try {
			if (zk != null) {
				zk.close();
			}
		} finally {
			zk = null;
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

	/**
	 * Initializing method.
	 * 
	 * @throws Exception
	 */
	public void init() throws Exception {
		_connect();
		if (cacheEnabled) {
			cache = CacheBuilder
					.newBuilder()
					.concurrencyLevel(
							Runtime.getRuntime().availableProcessors())
					.maximumSize(Integer.MAX_VALUE)
					.expireAfterAccess(3600, TimeUnit.SECONDS)
					.build(new CacheLoader<String, byte[]>() {
						@Override
						public byte[] load(String path) throws Exception {
							byte[] result = _read(path);
							if (result == null) {
								throw new ZooKeeperException.NodeNotFoundException();
							}
							return result;
						}
					});
		}
	}

	/**
	 * Destroying method.
	 * 
	 * @throws Exception
	 */
	public void destroy() throws Exception {
		_close();
	}

	private void _eventNodeChanged(WatchedEvent event)
			throws ZooKeeperException {
		Watcher.Event.EventType type = event.getType();
		String path = event.getPath();
		switch (type) {
		case NodeDataChanged: {
			// reload node data
			byte[] data = _read(path);
			cache.put(path, data);
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
		// System.out.println(event);
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
}
