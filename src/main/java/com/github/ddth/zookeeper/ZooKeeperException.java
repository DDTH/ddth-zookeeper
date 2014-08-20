package com.github.ddth.zookeeper;

/**
 * Throws to indicate there has been an exception while interacting with
 * ZooKeeper server.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 * @since 0.3.1.2 ZooKeeperException extends RuntimeException
 */
public class ZooKeeperException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public static class NodeNotFoundException extends ZooKeeperException {
        private static final long serialVersionUID = 1L;
    }

    public static class ClientDisconnectedException extends ZooKeeperException {
        private static final long serialVersionUID = 1L;
    }

    public ZooKeeperException() {
    }

    public ZooKeeperException(String message) {
        super(message);
    }

    public ZooKeeperException(Throwable cause) {
        super(cause);
    }

    public ZooKeeperException(String message, Throwable cause) {
        super(message, cause);
    }

}
