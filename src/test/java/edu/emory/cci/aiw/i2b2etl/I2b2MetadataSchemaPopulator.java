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
import java.sql.SQLException;

/**
 *
 * @author Andrew Post
 */
public final class I2b2MetadataSchemaPopulator extends AbstractH2Populator {

    private static File dbFile;

    public File populate() throws IOException, SQLException {
        synchronized (I2b2MetadataSchemaPopulator.class) {
            if (dbFile == null) {
                dbFile = File.createTempFile("i2b2-ksb", ".db");
                updateLiquibaseChangeLog(dbFile, "src/main/resources/dbmigration/create-table-access-changelog.xml");
                updateLiquibaseChangeLog(dbFile, "src/main/resources/dbmigration/i2b2-meta-schema-changelog.xml");
                populate(dbFile, "INIT=RUNSCRIPT FROM 'src/test/resources/i2b2-1.sql'\\;RUNSCRIPT FROM 'src/test/resources/i2b2-2.sql'");
            }
            return dbFile;
        }
    }
}
