package edu.emory.cci.aiw.i2b2etl.configuration;

/**
 *
 * @author Andrew Post
 */
public class ConfigurationReadException extends Exception {

    public ConfigurationReadException(Throwable thrwbl) {
        super(thrwbl);
    }

    public ConfigurationReadException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public ConfigurationReadException(String string) {
        super(string);
    }

    public ConfigurationReadException() {
    }
    
}
