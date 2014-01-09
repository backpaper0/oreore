package oreore.mock;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

public class MockDataSourceProvider implements InvocationHandler {

    private static AtomicInteger idCounter = new AtomicInteger(0);

    private final String id;

    private final DataSource dataSource;

    private final List<MockConnectionProvider> mockConnectionProviders = new ArrayList<>();

    public MockDataSourceProvider() {
        this.id = String.valueOf(idCounter.incrementAndGet());
        ClassLoader loader = getClass().getClassLoader();
        Class<?>[] interfaces = { DataSource.class, Connection.class };
        this.dataSource = (DataSource) Proxy.newProxyInstance(loader,
                interfaces, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {

        if (method.equals(DataSource.class.getMethod("getConnection"))) {
            MockConnectionProvider connectionHandler = new MockConnectionProvider(
                    id);
            mockConnectionProviders.add(connectionHandler);
            return connectionHandler.get();
        }

        throw new UnsupportedOperationException(method.toGenericString());
    }

    public DataSource get() {
        return dataSource;
    }

    public List<MockConnectionProvider> getMockConnectionProviders() {
        return mockConnectionProviders;
    }
}
