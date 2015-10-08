package edu.emory.cci.aiw.i2b2etl.dest.config;

import org.arp.javautil.sql.ConnectionSpec;
import org.arp.javautil.sql.DatabaseAPI;
import org.arp.javautil.sql.InvalidConnectionSpecArguments;

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

public abstract class DatabaseSpec {
    private final String user;
    private final String passwd;
    private final String connect;

    DatabaseSpec(String connect, String user, String passwd) {
        this.user = user;
        this.passwd = passwd;
        this.connect = connect;
    }

    public String getUser() {
        return user;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getConnect() {
        return connect;
    }
    
    public abstract DatabaseAPI getDatabaseAPI();
    
    /**
     * Gets a connection spec configured with the user, password and
     * connection string specified. Connections created from this connection
     * spec will have auto commit turned off.
     * 
     * @return a {@link ConnectionSpec}.
     */
    public ConnectionSpec toConnectionSpec() {
        try {
            return getDatabaseAPI().newConnectionSpecInstance(getConnect(), getUser(), getPasswd(), false);
        } catch (InvalidConnectionSpecArguments ex) {
            throw new AssertionError(ex);
        }
    }
    
}
