package oreore.tx;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public class LocalTransaction {

    private final class Context {

        private Connection con;

        private boolean rollbackOnly;

        public void begin() throws SQLException {
            con = dataSource.getConnection();
            con.setAutoCommit(false);
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
            return con;
        }
    }

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
        context.begin();
        contexts.set(context);
    }

    public void commit() throws SQLException {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        context.commit();
        contexts.remove();
    }

    public void rollback() throws SQLException {
        Context context = contexts.get();
        if (context == null) {
            throw new IllegalStateException("Transaction must be begun");
        }
        context.rollback();
        contexts.remove();
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
        //TODO commitなどが出来ないようにラッパーを返す
        return context.getConnection();
    }
}
