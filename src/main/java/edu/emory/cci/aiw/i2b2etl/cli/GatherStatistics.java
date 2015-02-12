package edu.emory.cci.aiw.i2b2etl.cli;

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
import edu.emory.cci.aiw.i2b2etl.dest.I2b2Destination;
import edu.emory.cci.aiw.i2b2etl.dest.config.xml.XmlFileConfiguration;
import java.io.File;
import org.protempa.dest.Destination;
import org.protempa.dest.Statistics;
import org.protempa.dest.StatisticsException;

/**
 *
 * @author Andrew Post
 */
public class GatherStatistics {

    public static void main(String[] args) {
        File configDir = new File(args[0]);
        if (!configDir.exists()) {
            System.err.println("Specified i2b2ConfigDir " + configDir.getName() + " does not exist");
            System.exit(1);
        }
        if (!configDir.isDirectory()) {
            System.exit(2);
        }
        int totalKeys = 0;
        for (File confXML : configDir.listFiles()) {
            Destination destination = new I2b2Destination(new XmlFileConfiguration(confXML), I2b2Destination.DataInsertMode.TRUNCATE);
            try {
                Statistics stats = destination.getStatistics();
                int numberOfKeys = stats.getNumberOfKeys();
                System.out.println("I2b2 destination " + confXML.getName() + " has " + numberOfKeys + " keys");
                totalKeys += numberOfKeys;
            } catch (StatisticsException ex) {
                System.err.println("Error collecting statistics for i2b2 config " + confXML.getName() + ": " + ex.getMessage());
                System.exit(3);
            }
        }
        System.out.println("Total number of keys: " + totalKeys);
    }

}
