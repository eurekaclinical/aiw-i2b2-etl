/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.emory.cci.aiw.i2b2etl.table;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2013 Emory University
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
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author arpost
 */
public abstract class RecordHandler<E extends Record> {

    private boolean inited = false;
    private int batchNumber = 0;
    private int counter = 0;
    private final int batchSize = 1000;
    private int commitCounter = 0;
    private final int commitSize = 10000;
    private PreparedStatement ps;
    private final String statement;
    private Connection cn;
    private final Timestamp importTimestamp;

    public RecordHandler(ConnectionSpec connSpec, String statement) throws SQLException {
        this.cn = connSpec.getOrCreate();
        this.statement = statement;
        this.importTimestamp = new Timestamp(System.currentTimeMillis());
    }
    
    public final void close() throws SQLException {
        Logger logger = TableUtil.logger();
        try {
            if (this.ps != null) {
                try {
                    if (counter > 0) {
                        batchNumber++;
                        ps.executeBatch();
                        logger.log(Level.FINEST, "DB_OBX_BATCH={0}", batchNumber);
                    }
                    if (commitCounter > 0) {
                        cn.commit();
                    }
                    ps.close();
                    ps = null;
                } finally {
                    if (ps != null) {
                        try {
                            ps.close();
                        } catch (SQLException ignore) {
                        }
                    }
                }
            }
            this.cn.close();
            this.cn = null;
        } finally {
            if (this.cn != null) {
                try {
                    this.cn.close();
                } catch (SQLException ignore) {}
            }
        }
    }
    
    protected Timestamp importTimestamp() {
        return this.importTimestamp;
    }

    public final void insert(E record) throws SQLException {
        Logger logger = TableUtil.logger();
        if (record.isRejected()) {
            //logger.log(Level.WARNING, "Rejected fact {0}", obx);
        } else {
            try {
                if (!inited) {
                    ps = cn.prepareStatement(this.statement);
                    inited = true;
                }
                setParameters(ps, record);

                ps.addBatch();
                counter++;
                commitCounter++;
                if (counter >= batchSize) {
                    batchNumber++;
                    ps.executeBatch();
                    logger.log(Level.FINEST, "DB_OBX_BATCH={0}", batchNumber);
                    ps.clearBatch();
                    counter = 0;
                }
                if (commitCounter >= commitSize) {
                    cn.commit();
                    commitCounter = 0;
                }
                ps.clearParameters();
            } catch (SQLException e) {
                logger.log(Level.FINEST, "DB_OBX_BATCH_FAIL={0}", batchNumber);
                logger.log(Level.SEVERE, "Batch failed on ObservationFact. I2B2 will not be correct.", e);
                try {
                    ps.close();
                } catch (SQLException sqle) {
                }
                throw e;
            }
        }
    }

    protected abstract void setParameters(PreparedStatement statement, E record) throws SQLException;
}
