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
public class I2b2DataSchemaPopulator extends AbstractH2Populator {

    public File populate() throws IOException, SQLException {
        File dbFile = populate(File.createTempFile("i2b2-data", ".db"), "INIT=RUNSCRIPT FROM 'src/main/resources/sql/i2b2_data_tables_1_7_h2.sql'\\;RUNSCRIPT FROM 'src/main/resources/sql/mock_stored_procedures_h2.sql'");
        updateLiquibaseChangeLog(dbFile, "src/main/resources/dbmigration/i2b2-data-schema-changelog.xml");
        return dbFile;
    }

}
