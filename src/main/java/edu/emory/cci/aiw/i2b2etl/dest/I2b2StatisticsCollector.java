package edu.emory.cci.aiw.i2b2etl.dest;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2014 Emory University
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

import edu.emory.cci.aiw.i2b2etl.dest.config.Configuration;
import edu.emory.cci.aiw.i2b2etl.dest.config.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.dest.config.Database;
import edu.emory.cci.aiw.i2b2etl.dest.config.DatabaseSpec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;
import org.protempa.dest.DefaultStatisticsBuilder;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;

/**
 *
 * @author Andrew Post
 */
final class I2b2StatisticsCollector {
    private final ConnectionSpec dataConnectionSpec;

    I2b2StatisticsCollector(Configuration config) throws StatisticsException {
        try {
            config.init();
            Database databaseSection = config.getDatabase();
            DatabaseSpec dataSchemaSpec = databaseSection.getDataSpec();
            this.dataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(dataSchemaSpec.getConnect(), dataSchemaSpec.getUser(), dataSchemaSpec.getPasswd());
        } catch (InvalidConnectionSpecArguments | ConfigurationReadException ex) {
            throw new StatisticsException("Could not initialize statistics gathering", ex);
        }
    }

    Statistics collectStatistics() throws StatisticsException {
        int count;
        try (Connection conn = this.dataConnectionSpec.getOrCreate();
                Statement stmt = conn.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM PATIENT_DIMENSION");) {
            if (!resultSet.next()) {
                throw new AssertionError("No count retrieved for i2b2 destination");
            }
            count = resultSet.getInt(1);
        } catch (SQLException ex) {
            throw new StatisticsException("Could not retrieve statistics from i2b2 destination", ex);
        }
        DefaultStatisticsBuilder builder = new DefaultStatisticsBuilder();
        builder.setNumberOfKeys(count);
        return builder.toDefaultStatistics();
    }

}
