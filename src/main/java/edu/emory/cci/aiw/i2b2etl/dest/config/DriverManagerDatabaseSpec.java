package edu.emory.cci.aiw.i2b2etl.dest.config;

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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.arp.javautil.sql.DatabaseAPI;

/**
 *
 * @author Andrew Post
 */
public class DriverManagerDatabaseSpec extends DatabaseSpec {
    private static final Logger LOGGER = Logger.getLogger(DriverManagerDatabaseSpec.class.getName());
    
    private static final String[] jdbcDriverClassNames = {
        "oracle.jdbc.OracleDriver",
        "org.postgresql.Driver",
        "org.h2.Driver"
    };

    public DriverManagerDatabaseSpec(String connect, String user, String passwd) {
        super(connect, user, passwd);
        loadDrivers();
    }

    @Override
    public DatabaseAPI getDatabaseAPI() {
        return DatabaseAPI.DRIVERMANAGER;
    }

    private static void loadDrivers() {
        for (String driverClassName : jdbcDriverClassNames) {
            try {
                Class.forName(driverClassName);
                LOGGER.log(Level.FINE, "JDBC driver {0} found", driverClassName);
            } catch (ClassNotFoundException ex) {
                LOGGER.log(Level.FINE, "No JDBC driver {0} found", driverClassName);
            }
        }
    }
    
}
