package oreore.dbcp;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import java.sql.Connection;
import java.sql.SQLException;
import oreore.mock.MockDataSourceProvider;
import org.junit.Test;

public class PoolingDataSourceFactoryTest {

    @Test
    public void testCreate() throws SQLException {
        PoolingDataSourceFactory factory = new PoolingDataSourceFactory();
        try (ConnectionPool cp = new ConnectionPool()) {
            cp.setMaxPoolSize(1);
            MockDataSourceProvider provider = new MockDataSourceProvider();
            cp.setDataSource(provider.get());
            try (PoolingDataSource dataSource = factory.create(cp)) {
                Connection con1 = dataSource.getConnection();
                Connection con2 = dataSource.getConnection();

                con1.close();
                con2.close();

                Connection con3 = dataSource.getConnection();
                Connection con4 = dataSource.getConnection();

                assertThat("1:2", con1, not(con2));
                assertThat("1:3", con1, is(con3));
                assertThat("1:4", con1, not(con4));
                assertThat("2:3", con2, not(con3));
                assertThat("2:4", con2, not(con4));
                assertThat("3:4", con3, not(con4));

                con3.close();
                con4.close();
            }
        }
    }

}
