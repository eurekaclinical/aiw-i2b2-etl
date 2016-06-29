package edu.emory.cci.aiw.i2b2etl;

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
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.FileSystemResourceAccessor;

/**
 *
 * @author Andrew Post
 */
public abstract class AbstractH2Populator {

    protected void populate(File dbFile, String args) throws IOException, SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath() + ";LOG=0;CACHE_SIZE=262400;LOCK_MODE=0;UNDO_LOG=0" + (args != null ? ";" + args : ""));
                Statement stmt = connection.createStatement()) {
            stmt.execute("SHUTDOWN DEFRAG");
        }
    }

    protected void outputSqlFromLiquibaseChangeLog(File dbFile, String changeLogResource, Writer writer) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath())) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try {
                Liquibase liquibase = new Liquibase(changeLogResource, new FileSystemResourceAccessor(), database);
                liquibase.update((Contexts) null, writer);
                database.close();
                database = null;
            } catch (DatabaseException ex) {
                if (database != null) {
                    database.close();
                }
                throw new IOException(ex);
            }
        } catch (LiquibaseException ex) {
            throw new IOException(ex);
        }
    }

    protected void updateLiquibaseChangeLog(File dbFile, String changeLog) throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:" + dbFile.getAbsolutePath())) {
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            try {
                Liquibase liquibase = new Liquibase(changeLog, new FileSystemResourceAccessor(), database);
                liquibase.update((Contexts) null);
                database.close();
                database = null;
            } catch (DatabaseException ex) {
                if (database != null) {
                    database.close();
                }
                throw new IOException(ex);
            }
        } catch (LiquibaseException ex) {
            throw new IOException(ex);
        }
    }
}
