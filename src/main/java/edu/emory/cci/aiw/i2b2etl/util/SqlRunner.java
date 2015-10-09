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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author arpost
 */
class SqlRunner extends Thread {

    private static final Logger LOGGER = Logger.getLogger(SqlRunner.class.getName());
    private final int batchSize = 1000;
    private final int commitSize = 10000;
    private boolean stop;
    private SQLException exception;
    private int commitCounter = 0;
    private PreparedStatement ps;
    private final boolean commit;

    SqlRunner(PreparedStatement preparedStatement, boolean commit) {
        assert preparedStatement != null : "preparedStatement cannot be null";
        this.ps = preparedStatement;
        this.commit = commit;
    }

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
                                ps.getConnection().commit();
                            }
                            commitCounter = 0;
                        }
                        ps.clearParameters();
                    }
                }
            }
        } catch (SQLException ex) {
            rollback(ex);
            this.exception = ex;
            logMultipleSqlExceptions(ex);
        } catch (InterruptedException ex) {
            rollback(ex);
            LOGGER.log(Level.FINE, "Batch inserter was interrupted: {0}", ex);
        }
    }

    void incrementCommitCounter() {
        this.commitCounter++;
    }

    int getCommitCounter() {
        return commitCounter;
    }

    int getBatchSize() {
        return batchSize;
    }

    SQLException getException() {
        return exception;
    }

    void requestStop() {
        this.stop = true;
    }

    private void logMultipleSqlExceptions(SQLException sqlException) {
        if (sqlException.getNextException() != null) {
            LOGGER.log(Level.SEVERE, "Error doing batch insert, threw multiple SQL exceptions. The first one will propagate up the stack, but here are all of them so you can see them:");
            SQLException sqle = sqlException;
            int i = 1;
            do {
                LOGGER.log(Level.SEVERE, "Error " + i++, sqle);
            } while ((sqle = sqle.getNextException()) != null);
        }
    }

    private void rollback(Throwable throwable) {
        if (commit) {
            try {
                ps.getConnection().rollback();
            } catch (SQLException ignore) {
                throwable.addSuppressed(ignore);
            }
        }
    }

}
