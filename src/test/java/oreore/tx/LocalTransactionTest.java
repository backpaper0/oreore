package oreore.tx;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import oreore.mock.MockConnectionProvider;
import oreore.mock.MockDataSourceProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalTransactionTest {

    private MockDataSourceProvider provider;

    private LocalTransaction tx;

    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        provider = new MockDataSourceProvider();
        tx = new LocalTransaction(provider.get());
        executor = Executors.newFixedThreadPool(10);
    }

    @After
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void test_begin_commit() throws Exception {
        List<MockConnectionProvider> mockConnectionProviders = provider
                .getMockConnectionProviders();
        assertThat(mockConnectionProviders.size(), is(0));

        tx.begin();

        assertThat(mockConnectionProviders.size(), is(1));
        MockConnectionProvider mockConnectionProvider = mockConnectionProviders
                .get(0);
        assertThat(mockConnectionProvider.isAutoCommit(), is(false));
        assertThat(mockConnectionProvider.isClosed(), is(false));
        assertThat(mockConnectionProvider.getCommitCount(), is(0));
        assertThat(mockConnectionProvider.getRollbackCount(), is(0));

        tx.commit();

        assertThat(mockConnectionProviders.size(), is(1));
        //assertThat(mockConnectionProvider.isAutoCommit(), is(true));
        assertThat(mockConnectionProvider.isClosed(), is(true));
        assertThat(mockConnectionProvider.getCommitCount(), is(1));
        assertThat(mockConnectionProvider.getRollbackCount(), is(0));
    }

    @Test
    public void test_begin_rollback() throws Exception {
        List<MockConnectionProvider> mockConnectionProviders = provider
                .getMockConnectionProviders();
        assertThat(mockConnectionProviders.size(), is(0));

        tx.begin();

        assertThat(mockConnectionProviders.size(), is(1));
        MockConnectionProvider mockConnectionProvider = mockConnectionProviders
                .get(0);
        assertThat(mockConnectionProvider.isAutoCommit(), is(false));
        assertThat(mockConnectionProvider.isClosed(), is(false));
        assertThat(mockConnectionProvider.getCommitCount(), is(0));
        assertThat(mockConnectionProvider.getRollbackCount(), is(0));

        tx.rollback();

        assertThat(mockConnectionProviders.size(), is(1));
        //assertThat(mockConnectionProvider.isAutoCommit(), is(true));
        assertThat(mockConnectionProvider.isClosed(), is(true));
        assertThat(mockConnectionProvider.getCommitCount(), is(0));
        assertThat(mockConnectionProvider.getRollbackCount(), is(1));
    }

    @Test(expected = IllegalStateException.class)
    public void test_begin_begin() throws Exception {
        tx.begin();
        tx.begin();
    }

    @Test(expected = IllegalStateException.class)
    public void test_begin_commit_commit() throws Exception {
        tx.begin();
        tx.commit();
        tx.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void test_begin_rollback_rollback() throws Exception {
        tx.begin();
        tx.rollback();
        tx.rollback();
    }

    @Test(expected = IllegalStateException.class)
    public void test_begin_commit_rollback() throws Exception {
        tx.begin();
        tx.commit();
        tx.rollback();
    }

    @Test(expected = IllegalStateException.class)
    public void test_begin_rollback_commit() throws Exception {
        tx.begin();
        tx.rollback();
        tx.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void test_commit() throws Exception {
        tx.commit();
    }

    @Test(expected = IllegalStateException.class)
    public void test_rollback() throws Exception {
        tx.rollback();
    }

    @Test
    public void test_concurrent() throws Exception {
        final CountDownLatch begin = new CountDownLatch(1);
        final CountDownLatch commit = new CountDownLatch(1);
        final CountDownLatch began = new CountDownLatch(1);
        final CountDownLatch committed = new CountDownLatch(1);

        final long timeout = 1;
        final TimeUnit unit = TimeUnit.SECONDS;

        executor.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {

                assertTrue(begin.await(timeout, unit));
                tx.begin();
                began.countDown();

                assertTrue(commit.await(timeout, unit));
                tx.commit();
                committed.countDown();

                return null;
            }
        });

        List<MockConnectionProvider> mockConnectionProviders = provider
                .getMockConnectionProviders();
        assertThat(mockConnectionProviders.size(), is(0));

        tx.begin();

        assertThat(mockConnectionProviders.size(), is(1));
        MockConnectionProvider mockConnectionProvider1 = mockConnectionProviders
                .get(0);

        begin.countDown();
        assertTrue(began.await(timeout, unit));

        assertThat(mockConnectionProviders.size(), is(2));
        MockConnectionProvider mockConnectionProvider2 = mockConnectionProviders
                .get(1);

        assertThat(mockConnectionProvider1.getCommitCount(), is(0));
        assertThat(mockConnectionProvider2.getCommitCount(), is(0));

        tx.commit();

        assertThat(mockConnectionProvider1.getCommitCount(), is(1));
        assertThat(mockConnectionProvider2.getCommitCount(), is(0));

        commit.countDown();
        assertTrue(committed.await(timeout, unit));

        assertThat(mockConnectionProvider1.getCommitCount(), is(1));
        assertThat(mockConnectionProvider2.getCommitCount(), is(1));
    }
}
