package edu.emory.cci.aiw.i2b2etl.util;

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
import java.sql.SQLException;
import org.arp.javautil.sql.ConnectionSpec;

/**
 *
 * @param <E> any object representing a database record.
 * 
 * @author Andrew Post
 */
public abstract class ConnectionSpecRecordHandler<E> extends RecordHandler<E> {

    public ConnectionSpecRecordHandler(ConnectionSpec connSpec, String statement) throws SQLException {
        super(connSpec.getOrCreate(), statement);
    }

    @Override
    public void close() throws SQLException {
        Connection cn = getConnection();
        try {
            super.close();
            cn.close();
            cn = null;
        } finally {
            if (cn != null) {
                try {
                    cn.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

}
