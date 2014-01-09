package edu.emory.cci.aiw.i2b2etl;

import edu.emory.cci.aiw.i2b2etl.configuration.ConfigurationReadException;
import edu.emory.cci.aiw.i2b2etl.configuration.ConfigurationReader;
import edu.emory.cci.aiw.i2b2etl.configuration.DatabaseSection;
import java.io.File;
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

    I2b2StatisticsCollector(File confFile) throws StatisticsException {
        
        ConfigurationReader configurationReader = new ConfigurationReader(confFile);
        try {
            configurationReader.read();
        } catch (ConfigurationReadException ex) {
            throw new StatisticsException("Could not initialize statistics gathering", ex);
        }
        DatabaseSection databaseSection = configurationReader.getDatabaseSection();

        DatabaseSection.DatabaseSpec dataSchemaSpec = databaseSection.get("dataschema");
        try {
            this.dataConnectionSpec = DatabaseAPI.DRIVERMANAGER.newConnectionSpecInstance(dataSchemaSpec.connect, dataSchemaSpec.user, dataSchemaSpec.passwd);
        } catch (InvalidConnectionSpecArguments ex) {
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
