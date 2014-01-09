package oreore.tx;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.sql.Connection;
import oreore.mock.MockDataSourceProvider;
import org.junit.Test;

public class LocalTransactionalDataSourceFactoryTest {

    @Test
    public void testGetConnection() throws Exception {
        LocalTransactionalDataSourceFactory factory = new LocalTransactionalDataSourceFactory();
        MockDataSourceProvider mockDataSourceProvider = new MockDataSourceProvider();
        LocalTransactionalDataSource dataSource = factory
                .create(mockDataSourceProvider.get());
        LocalTransaction tx = dataSource.getTransaction();

        try {
            dataSource.getConnection();
            fail();
        } catch (IllegalStateException e) {}

        tx.begin();

        Connection con1 = dataSource.getConnection();
        assertThat(con1.getAutoCommit(), is(false));

        Connection con2 = dataSource.getConnection();

        assertThat(con1, is(con2));

        tx.rollback();

        assertThat(con1.isClosed(), is(true));

        try {
            dataSource.getConnection();
            fail();
        } catch (IllegalStateException e) {}
    }

}
