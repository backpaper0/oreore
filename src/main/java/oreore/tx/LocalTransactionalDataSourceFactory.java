package oreore.tx;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import javax.sql.DataSource;

public class LocalTransactionalDataSourceFactory {

    private static class TransactionalDataSourceHandler implements
            InvocationHandler {

        private final DataSource dataSource;

        private final LocalTransaction transaction;

        private final LocalTransactionalDataSource proxy;

        public TransactionalDataSourceHandler(DataSource dataSource,
                ClassLoader loader) {
            this.dataSource = dataSource;
            this.transaction = new LocalTransaction(dataSource);
            this.proxy = (LocalTransactionalDataSource) Proxy
                    .newProxyInstance(
                            loader,
                            new Class<?>[] { LocalTransactionalDataSource.class },
                            this);
        }

        public LocalTransactionalDataSource get() {
            return proxy;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {

            if (method.equals(DataSource.class.getMethod("getConnection"))) {
                return transaction.getConnection();
            }

            if (method.equals(LocalTransactionalDataSource.class
                    .getMethod("getTransaction"))) {
                return transaction;
            }

            return method.invoke(dataSource, args);
        }
    }

    public LocalTransactionalDataSource create(DataSource dataSource) {
        ClassLoader loader = getClass().getClassLoader();
        return new TransactionalDataSourceHandler(dataSource, loader).get();
    }
}
