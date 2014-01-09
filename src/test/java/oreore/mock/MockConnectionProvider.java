package oreore.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConnectionProvider implements InvocationHandler {

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final String id;
    private final Connection con;
    private boolean closed = false;
    private int commitCount = 0;
    private int rollbackCount = 0;
    private boolean autoCommit = true;

    public MockConnectionProvider(String parentId) {
        this.id = parentId + ":" + idCounter.incrementAndGet();
        ClassLoader loader = getClass().getClassLoader();
        Class<?>[] interfaces = { Connection.class };
        this.con = (Connection) Proxy
                .newProxyInstance(loader, interfaces, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {

        if (method.equals(Connection.class.getMethod("isClosed"))) {
            return closed;
        } else if (method.equals(Connection.class.getMethod("close"))) {
            closed = true;
            return null;
        } else if (method.equals(Connection.class.getMethod("commit"))) {
            if (closed) {
                throw new SQLException("Connection is already closed");
            } else if (autoCommit) {
                throw new SQLException(
                        "Connection must not be auto commit mode");
            }
            commitCount++;
            return null;
        } else if (method.equals(Connection.class.getMethod("rollback"))) {
            if (closed) {
                throw new SQLException("Connection is already closed");
            } else if (autoCommit) {
                throw new SQLException(
                        "Connection must not be auto commit mode");
            }
            rollbackCount++;
            return null;
        } else if (method.equals(Connection.class.getMethod("setAutoCommit",
                boolean.class))) {
            if (closed) {
                throw new SQLException("Connection is already closed");
            }
            autoCommit = (boolean) args[0];
            return null;
        } else if (method.equals(Connection.class.getMethod("getAutoCommit"))) {
            if (closed) {
                throw new SQLException("Connection is already closed");
            }
            return autoCommit;
        }

        if (method.equals(Object.class.getMethod("toString"))) {
            return "Connection(" + id + ")";
        } else if (method.equals(Object.class.getMethod("hashCode"))) {
            return id.hashCode();
        } else if (method
                .equals(Object.class.getMethod("equals", Object.class))) {
            Object other = args[0];
            if (other != null && Proxy.isProxyClass(other.getClass())) {
                InvocationHandler handler = Proxy.getInvocationHandler(other);
                if (MockConnectionProvider.class.isAssignableFrom(handler
                        .getClass())) {
                    return id
                            .equals(MockConnectionProvider.class.cast(handler).id);
                }
            }
            return false;
        }

        throw new UnsupportedOperationException(method.toGenericString());
    }

    public Connection get() {
        return con;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getCommitCount() {
        return commitCount;
    }

    public int getRollbackCount() {
        return rollbackCount;
    }
}
