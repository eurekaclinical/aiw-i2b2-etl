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
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;

/**
 *
 * @author Andrew Post
 */
public class I2b2MetadataSchemaPopulator extends AbstractH2Populator {

    public File populate() throws IOException, SQLException {
        File createTempFile = File.createTempFile("i2b2metaschemapopulator", ".sql");
        File dbFile = File.createTempFile("i2b2-ksb", ".db");
        try (Writer w = new FileWriter(createTempFile)) {
            outputSqlFromLiquibaseChangeLog(dbFile, "src/main/resources/dbmigration/i2b2-meta-schema-changelog.xml", w);
        }
        return populate(dbFile, "INIT=RUNSCRIPT FROM 'src/main/resources/sql/i2b2_meta_eureka_table_1_7_h2.sql'\\;RUNSCRIPT FROM '" + createTempFile.getAbsolutePath() + "'\\;RUNSCRIPT FROM 'src/test/resources/i2b2.sql'");
    }
}
