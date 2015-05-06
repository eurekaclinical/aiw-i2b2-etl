package edu.emory.cci.aiw.i2b2etl.util;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2015 Emory University
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Inserts a record into a database using prepared statements in batch mode. The
 * actual batch inserts occur in a separate thread.
 *
 * @author Andrew Post
 */
public abstract class RecordHandler<E> implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(RecordHandler.class.getName());

    private boolean inited;
    private int counter = 0;
    private final int batchSize = 1000;
    private volatile int commitCounter = 0;
    private final int commitSize = 10000;
    private PreparedStatement ps;
    private final String statement;
    private Connection cn;
    private final Timestamp importTimestamp;
    private final boolean commit;
    private final SqlRunner executor;
    private SQLException sqlExceptionThrownInExecutor;

    public RecordHandler(Connection connection, String statement) throws SQLException {
        this(connection, statement, true);
    }

    public RecordHandler(Connection connection, String statement, boolean commit) throws SQLException {
        this.cn = connection;
        this.statement = statement;
        this.importTimestamp = new Timestamp(System.currentTimeMillis());
        this.commit = commit;
        this.executor = new SqlRunner();
    }

    public void insert(E record) throws SQLException {
        if (this.sqlExceptionThrownInExecutor != null) {
            throw new SQLException("Previous call to insert caused the following exception to be thrown: " + this.sqlExceptionThrownInExecutor);
        }
        if (record != null) {
            try {
                if (!inited) {
                    ps = cn.prepareStatement(this.statement);
                    inited = true;
                    this.executor.start();
                }
                synchronized (this.executor) {
                    setParameters(ps, record);
                    ps.addBatch();
                    counter++;
                    commitCounter++;
                    if (counter >= batchSize) {
                        this.executor.notify();
                        counter = 0;
                    }
                }
            } catch (SQLException e) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException sqle) {
                    }
                }
                throw e;
            }
        }
    }

    private class SqlRunner extends Thread {

        private volatile boolean stop;

        @Override
        public void run() {
            try {
                synchronized (this) {
                    while (!isInterrupted() && !stop) {
                        wait();
                        if (!stop) {
                            ps.executeBatch();
                            ps.clearBatch();
                            if (commitCounter >= commitSize) {
                                if (commit) {
                                    cn.commit();
                                }
                                commitCounter = 0;
                            }
                            ps.clearParameters();
                        }
                    }
                }
            } catch (SQLException ex) {
                RecordHandler.this.sqlExceptionThrownInExecutor = ex;
            } catch (InterruptedException ex) {
                LOGGER.log(Level.FINE, "Batch inserter was interrupted: {0}", ex);
            }
        }

        public void requestStop() {
            this.stop = true;
        }
    }

    protected abstract void setParameters(PreparedStatement statement, E record) throws SQLException;

    protected Connection getConnection() {
        return this.cn;
    }

    @Override
    public void close() throws SQLException {
        if (this.sqlExceptionThrownInExecutor != null) {
            throw new SQLException("Previous call to insert caused the following exception to be thrown: " + this.sqlExceptionThrownInExecutor);
        }
        if (this.ps != null) {
            try {
                synchronized (this.executor) {
                    this.executor.requestStop();
                    this.executor.notify();
                }
                this.executor.join();
                if (counter > 0) {
                    ps.executeBatch();
                }
                if (commit && commitCounter > 0) {
                    cn.commit();
                }
                ps.close();
                ps = null;
            } catch (InterruptedException ex) {
                LOGGER.log(Level.FINE, "Close was interrupted: {0}", ex);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException ignore) {
                    }
                }
            }
        }
    }

    protected Timestamp importTimestamp() {
        return this.importTimestamp;
    }
}
