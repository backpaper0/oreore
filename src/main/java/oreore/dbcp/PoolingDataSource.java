package oreore.dbcp;

import java.sql.SQLException;
import javax.sql.DataSource;

public interface PoolingDataSource extends DataSource, AutoCloseable {

    @Override
    void close() throws SQLException;
}
