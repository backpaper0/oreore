package oreore.dbcp;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public class PoolingDataSourceFactory {

    private static class PoolingDataSourceHandler implements InvocationHandler {

        private final ConnectionPool cp;
        private final ClassLoader loader;
        private final PoolingDataSource proxy;

        public PoolingDataSourceHandler(ConnectionPool cp, ClassLoader loader) {
            this.cp = cp;
            this.loader = loader;
            this.proxy = (PoolingDataSource) Proxy.newProxyInstance(loader,
                    new Class<?>[] { PoolingDataSource.class }, this);
        }

        public PoolingDataSource get() {
            return proxy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {

            if (method.equals(DataSource.class.getMethod("getConnection"))) {
                return new ConnectionHandler(cp, loader).get();
            }

            if (method.equals(PoolingDataSource.class.getMethod("close"))) {
                cp.close();
                return null;
            }

            return method.invoke(cp.getDataSource(), args);
        }
    }

    private static class ConnectionHandler implements InvocationHandler {

        private final ConnectionPool cp;
        private final Connection con;
        private final Connection proxy;

        public ConnectionHandler(ConnectionPool cp, ClassLoader loader)
                throws SQLException {
            this.cp = cp;
            this.con = this.cp.checkOut();
            proxy = (Connection) Proxy.newProxyInstance(loader,
                    new Class<?>[] { Connection.class }, this);
        }

        public Connection get() {
            return proxy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            if (method.equals(Connection.class.getMethod("close"))) {
                cp.checkIn(con);
                return null;
            }

            if (method.equals(Object.class.getMethod("toString"))) {
                return "Pooled(" + con + ")";
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

    public PoolingDataSource create(ConnectionPool cp) {
        ClassLoader loader = getClass().getClassLoader();
        return new PoolingDataSourceHandler(cp, loader).get();
    }
}
