package edu.emory.cci.aiw.i2b2etl.dest.table;

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
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @author Andrew Post
 */
public abstract class RecordHandler<E extends Record> implements AutoCloseable {

    private boolean inited = false;
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

    @Override
    public void close() throws SQLException {
        try {
            if (this.ps != null) {
                try {
                    if (counter > 0) {
                        ps.executeBatch();
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
                } catch (SQLException ignore) {
                }
            }
        }
    }

    protected Timestamp importTimestamp() {
        return this.importTimestamp;
    }

    public void insert(E record) throws SQLException {
        if (record != null) {
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
                    ps.executeBatch();
                    ps.clearBatch();
                    counter = 0;
                }
                if (commitCounter >= commitSize) {
                    cn.commit();
                    commitCounter = 0;
                }
                ps.clearParameters();
            } catch (SQLException e) {
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
