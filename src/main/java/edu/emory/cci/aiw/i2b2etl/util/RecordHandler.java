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

    private boolean init;
    private int counter = 0;
    private volatile PreparedStatement ps;
    private final String statement;
    private Connection cn;
    private final Timestamp importTimestamp;
    private final boolean commit;
    private final SqlRunner executor;

    public RecordHandler(Connection connection, String statement) throws SQLException {
        this(connection, statement, true);
    }

    public RecordHandler(Connection connection, String statement, boolean commit) throws SQLException {
        if (connection == null) {
            throw new IllegalArgumentException("connection cannot be null");
        }
        if (statement == null) {
            throw new IllegalArgumentException("statement cannot be null");
        }
        this.cn = connection;
        this.statement = statement;
        this.importTimestamp = new Timestamp(System.currentTimeMillis());
        this.commit = commit;

        this.ps = this.cn.prepareStatement(this.statement);
        this.executor = new SqlRunner(this.ps, this.commit);
    }

    public void insert(E record) throws SQLException {
        if (!this.init) {
            this.executor.start();
            this.init = true;
        }
        SQLException exceptionThrown = this.executor.getException();
        if (exceptionThrown != null) {
            throw exceptionThrown;
        }
        if (record != null) {
            try {
                synchronized (this.executor) {
                    setParameters(this.ps, record);
                    this.ps.addBatch();
                    this.counter++;
                    this.executor.incrementCommitCounter();
                    if (this.counter >= this.executor.getBatchSize()) {
                        this.executor.notify();
                        this.counter = 0;
                    }
                }
            } catch (SQLException e) {
                if (this.ps != null) {
                    try {
                        this.ps.close();
                    } catch (SQLException sqle) {
                    }
                }
                throw e;
            }
        }
    }

    protected abstract void setParameters(PreparedStatement statement, E record) throws SQLException;

    protected Connection getConnection() {
        return this.cn;
    }

    @Override
    public void close() throws SQLException {
        SQLException exceptionThrown = this.executor.getException();
        try {
            if (exceptionThrown != null) {
                throw exceptionThrown;
            }
        } finally {
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
                    if (commit && this.executor.getCommitCounter() > 0) {
                        cn.commit();
                    }
                    ps.close();
                    ps = null;
                } catch (SQLException ex) {
                    rollback(ex);
                    if (exceptionThrown != null) {
                        exceptionThrown.addSuppressed(ex);
                    } else {
                        throw ex;
                    }
                } catch (InterruptedException ex) {
                    rollback(ex);
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
    }

    protected Timestamp importTimestamp() {
        return this.importTimestamp;
    }
    
    private void rollback(Throwable throwable) {
        if (commit) {
            try {
                cn.rollback();
            } catch (SQLException ignore) {
                throwable.addSuppressed(ignore);
            }
        }
    }
}
