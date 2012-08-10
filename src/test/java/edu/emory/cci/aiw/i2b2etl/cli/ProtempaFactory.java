package edu.emory.cci.aiw.i2b2etl.cli;

import java.io.File;
import java.io.IOException;
import org.arp.javautil.io.IOUtil;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.SourceFactory;
import org.protempa.backend.BackendProviderSpecLoaderException;
import org.protempa.backend.Configurations;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.InvalidConfigurationException;
import org.protempa.bconfigs.commons.INICommonsConfigurations;

/**
 *
 * @author Andrew Post
 */
class ProtempaFactory {
    Protempa newInstance() throws IOException, 
            BackendProviderSpecLoaderException, ConfigurationsLoadException, 
            InvalidConfigurationException, ProtempaStartupException {
        File config = IOUtil.resourceToFile(
                "/protempa-config/protege-h2-test-config", 
                "protege-h2-test-config", null);
        Configurations configurations = 
                new INICommonsConfigurations(config.getParentFile());
        SourceFactory sourceFactory = 
                new SourceFactory(configurations, config.getName());
        
        // force the use of the H2 driver so we don't bother trying to load
        // others
        System.setProperty("protempa.dsb.relationaldatabase.sqlgenerator",
                "org.protempa.backend.dsb.relationaldb.H2SQLGenerator");
        
        return Protempa.newInstance(sourceFactory);
    }
}
