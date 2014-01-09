package oreore.tx;

import javax.sql.DataSource;

public interface LocalTransactionalDataSource extends DataSource {

    LocalTransaction getTransaction();
}
