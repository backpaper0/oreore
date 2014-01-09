package oreore.dbcp;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import oreore.mock.MockDataSourceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionPoolTest {

    private ExecutorService executor;
    private ConnectionPool cp;
    private MockDataSourceProvider provider;

    @Before
    public void setUp() throws Exception {
        cp = new ConnectionPool();
        provider = new MockDataSourceProvider();
        cp.setDataSource(provider.get());

        executor = Executors.newFixedThreadPool(100);
    }

    @After
    public void tearDown() throws Exception {
        if (cp != null) {
            cp.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void test_checkOut() throws Exception {
        Connection con = cp.checkOut();
        assertThat(con, is(notNullValue()));
    }

    @Test
    public void test_checkIn() throws Exception {
        Connection con1 = cp.checkOut();
        cp.checkIn(con1);
        Connection con2 = cp.checkOut();
        assertThat(con1, is(con2));
    }

    @Test
    public void test_minPoolSize() throws Exception {
        assertThat(cp.getPooledSize(), is(0));
        cp.setMinPoolSize(1);
        assertThat(cp.getPooledSize(), is(1));
    }

    @Test
    public void test_maxPoolSize() throws Exception {
        Connection con1 = cp.checkOut();
        Connection con2 = cp.checkOut();

        assertThat(cp.getPooledSize(), is(0));
        assertThat(con1.isClosed(), is(false));
        assertThat(con2.isClosed(), is(false));

        cp.setMaxPoolSize(1);
        cp.checkIn(con1);
        cp.checkIn(con2);

        assertThat(cp.getPooledSize(), is(1));
        assertThat(con1.isClosed(), is(false));
        assertThat(con2.isClosed(), is(true));
    }

    @Test
    public void test_maxPoolSize_2() throws Exception {
        Connection con1 = cp.checkOut();
        Connection con2 = cp.checkOut();

        assertThat(cp.getPooledSize(), is(0));
        assertThat(con1.isClosed(), is(false));
        assertThat(con2.isClosed(), is(false));

        cp.checkIn(con1);
        cp.checkIn(con2);
        cp.setMaxPoolSize(1);

        assertThat(cp.getPooledSize(), is(1));
        assertThat(con1.isClosed(), is(true));
        assertThat(con2.isClosed(), is(false));
    }

    @Test
    public void test_validate_setMaxPoolSize() throws Exception {
        cp.setMinPoolSize(2);
        try {
            cp.setMaxPoolSize(1);
            fail();
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void test_validate_setMinPoolSize() throws Exception {
        cp.setMaxPoolSize(1);
        try {
            cp.setMinPoolSize(2);
            fail();
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void test_dataSource() throws Exception {
        cp.setMinPoolSize(1);
        MockDataSourceProvider provider2 = new MockDataSourceProvider();
        cp.setDataSource(provider2.get());

        assertThat(provider.getMockConnectionProviders().size(), is(1));
        assertThat(provider.getMockConnectionProviders().get(0).get()
                .isClosed(), is(true));

        assertThat(provider2.getMockConnectionProviders().size(), is(1));
        assertThat(provider2.getMockConnectionProviders().get(0).get()
                .isClosed(), is(false));

        Connection con = cp.checkOut();
        assertThat(con, is(sameInstance(provider2.getMockConnectionProviders()
                .get(0).get())));
    }

    @Test
    public void test_timeout() throws Exception {
        cp.setTimeout(50, TimeUnit.MILLISECONDS);

        Connection con = cp.checkOut();
        cp.checkIn(con);

        assertThat(con.isClosed(), is(false));
        assertThat(cp.getPooledSize(), is(1));

        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(con.isClosed(), is(true));
        assertThat(cp.getPooledSize(), is(0));
    }

    @Test
    public void test_timeout_minPoolSize() throws Exception {
        cp.setTimeout(50, TimeUnit.MILLISECONDS);
        cp.setMinPoolSize(1);

        Connection con = cp.checkOut();
        cp.checkIn(con);

        assertThat(con.isClosed(), is(false));
        assertThat(cp.getPooledSize(), is(1));

        TimeUnit.MILLISECONDS.sleep(100);

        assertThat(con.isClosed(), is(true));
        assertThat(cp.getPooledSize(), is(1));
    }

    @Test
    public void test_close() throws Exception {
        Connection con1 = cp.checkOut();
        cp.checkIn(con1);

        assertThat(cp.getPooledSize(), is(1));
        assertThat(con1.isClosed(), is(false));

        cp.close();

        assertThat(cp.getPooledSize(), is(0));
        assertThat(con1.isClosed(), is(true));
    }

    @Test
    public void test_concurrent_checkIn() throws Exception {
        cp.setMaxPoolSize(1);

        List<Future<?>> futures = new ArrayList<>();
        final CountDownLatch gate = new CountDownLatch(1);
        final CountDownLatch ready = new CountDownLatch(100);
        for (int i = 0; i < 100; i++) {
            Callable<Void> task = new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    Connection con = cp.checkOut();
                    ready.countDown();
                    gate.await();
                    cp.checkIn(con);
                    return null;
                }
            };
            Future<Void> future = executor.submit(task);
            futures.add(future);
        }
        ready.await();
        gate.countDown();
        for (Future<?> future : futures) {
            future.get();
        }

        assertThat(cp.getPooledSize(), is(1));
    }
}
