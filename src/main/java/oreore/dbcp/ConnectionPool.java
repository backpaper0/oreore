package oreore.dbcp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class ConnectionPool implements AutoCloseable {

    private final class Pooled implements Callable<Void> {

        private final Connection con;
        private final ScheduledFuture<Void> future;

        public Pooled(Connection con) {
            this.con = con;
            this.future = executor.schedule(this, timeout,
                    TimeUnit.MILLISECONDS);
        }

        public Connection getConnection() {
            future.cancel(false);
            return con;
        }

        @Override
        public Void call() throws Exception {
            Lock lock = readWriteLock.writeLock();
            lock.lock();
            try {
                pool.remove(this);
                close(con);
                while (pool.size() < minPoolSize) {
                    pool.offer(new Pooled(dataSource.getConnection()));
                }
            } finally {
                lock.unlock();
            }
            return null;
        }
    }

    private static final Logger logger = Logger.getLogger(
            PoolingDataSource.class.getName(), "oreore");
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Queue<Pooled> pool = new LinkedList<>();
    private DataSource dataSource;
    private int minPoolSize = 0;
    private int maxPoolSize = Integer.MAX_VALUE;
    private long timeout = Long.MAX_VALUE;
    private final ScheduledExecutorService executor = Executors
            .newSingleThreadScheduledExecutor();

    private void close(Connection con) throws SQLException {
        con.close();
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "dbcp.physical.closed", new Object[] { con });
        }
    }

    private Connection open() throws SQLException {
        Connection con = dataSource.getConnection();
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, "dbcp.physical.opened", new Object[] { con });
        }
        return con;
    }

    public void setDataSource(DataSource dataSource) throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            while (pool.isEmpty() == false) {
                Pooled pooled = pool.poll();
                close(pooled.getConnection());
            }
            this.dataSource = dataSource;
            while (pool.size() < this.minPoolSize) {
                pool.offer(new Pooled(open()));
            }
        } finally {
            lock.unlock();
        }
    }

    public void setMinPoolSize(int minPoolSize) throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            if (minPoolSize > maxPoolSize) {
                throw new IllegalArgumentException();
            }
            this.minPoolSize = minPoolSize;
            while (pool.size() < this.minPoolSize) {
                pool.offer(new Pooled(open()));
            }
        } finally {
            lock.unlock();
        }
    }

    public void setMaxPoolSize(int maxPoolSize) throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            if (minPoolSize > maxPoolSize) {
                throw new IllegalArgumentException();
            }
            this.maxPoolSize = maxPoolSize;
            while (pool.size() > maxPoolSize) {
                Pooled pooled = pool.poll();
                close(pooled.getConnection());
            }
        } finally {
            lock.unlock();
        }
    }

    public Connection checkOut() throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            Pooled pooled = pool.poll();
            if (pooled != null) {
                return pooled.getConnection();
            }
            return open();
        } finally {
            lock.unlock();
        }
    }

    public void checkIn(Connection con) throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            if (pool.size() < maxPoolSize) {
                pool.offer(new Pooled(con));
            } else {
                close(con);
            }
        } finally {
            lock.unlock();
        }
    }

    public void setTimeout(int timeout, TimeUnit timeUnit) {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            this.timeout = timeUnit.toMillis(timeout);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            executor.shutdown();
            while (pool.isEmpty() == false) {
                Pooled pooled = pool.poll();
                close(pooled.getConnection());
            }
        } finally {
            lock.unlock();
        }
    }

    public int getPooledSize() {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return pool.size();
        } finally {
            lock.unlock();
        }
    }

    public DataSource getDataSource() {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return dataSource;
        } finally {
            lock.unlock();
        }
    }

    public int getMaxPoolSize() {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return maxPoolSize;
        } finally {
            lock.unlock();
        }
    }

    public int getMinPoolSize() {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return minPoolSize;
        } finally {
            lock.unlock();
        }
    }

    public long getTimeout() {
        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return timeout;
        } finally {
            lock.unlock();
        }
    }
}
