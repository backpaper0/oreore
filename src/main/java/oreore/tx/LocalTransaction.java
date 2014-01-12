package oreore.tx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class LocalTransaction {

    private final class Context {

        private final Connection con;
        private final ConnectionHandler handler;
        private boolean rollbackOnly;

        public Context() throws SQLException {
            con = dataSource.getConnection();
            con.setAutoCommit(false);

            ClassLoader loader = getClass().getClassLoader();
            handler = new ConnectionHandler(con, loader);
        }

        public void commit() throws SQLException {
            con.commit();
            con.setAutoCommit(true);
            con.close();
        }

        public void rollback() throws SQLException {
            con.rollback();
            con.setAutoCommit(true);
            con.close();
        }

        public void setRollbackOnly() {
            rollbackOnly = true;
        }

        public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        public Connection getConnection() {
            return handler.get();
        }
    }

    private static class ConnectionHandler implements InvocationHandler {

        private final Connection con;
        private final Connection proxy;

        public ConnectionHandler(Connection con, ClassLoader loader) {
            this.con = con;
            this.proxy = (Connection) Proxy.newProxyInstance(loader,
                    new Class<?>[] { Connection.class }, this);
        }

        public Connection get() {
            return proxy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {

            if (method.equals(Connection.class.getMethod("commit"))) {
                throw new SQLException();
            } else if (method.equals(Connection.class.getMethod("rollback"))) {
                throw new SQLException();
            } else if (method.equals(Connection.class.getMethod("close"))) {
                throw new SQLException();
            }

            if (method.equals(Object.class.getMethod("toString"))) {
                return "Transactional(" + con + ")";
            } else if (method.equals(Object.class.getMethod("hashCode"))) {
                return con.hashCode();
            } else if (method.equals(Object.class.getMethod("equals",
                    Object.class))) {
                Object other = args[0];
                if (other != null && Proxy.isProxyClass(other.getClass())) {
                    InvocationHandler handler = Proxy
                            .getInvocationHandler(other);
                    if (ConnectionHandler.class.isAssignableFrom(handler
                            .getClass())) {
                        return con
                                .equals(ConnectionHandler.class.cast(handler).con);
                    }
                }
                return false;
            }

            return method.invoke(con, args);
        }
    }

    private static final Logger logger = Logger.getLogger(
            LocalTransaction.class.getName(), "oreore");
    private final ThreadLocal<Context> contexts = new ThreadLocal<>();
    private final DataSource dataSource;

    public LocalTransaction(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void begin() throws SQLException {
        if (contexts.get() != null) {
            throw new IllegalStateException("Transaction is begun");
        }
        Context context = new Context();
        contexts.set(context);
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "tx.begun", new Object[] { context });
        }
    }

    public void commit() throws SQLException {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        context.commit();
        contexts.remove();
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "tx.committed", new Object[] { context });
        }
    }

    public void rollback() throws SQLException {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        context.rollback();
        contexts.remove();
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "tx.rolledback", new Object[] { context });
        }
    }

    public boolean isActive() {
        return contexts.get() != null;
    }

    public void setRollbackOnly() {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        context.setRollbackOnly();
    }

    public boolean isRollbackOnly() {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        return context.isRollbackOnly();
    }

    public Connection getConnection() {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        return context.getConnection();
    }
}
