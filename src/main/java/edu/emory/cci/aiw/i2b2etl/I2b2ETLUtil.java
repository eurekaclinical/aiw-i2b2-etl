package edu.emory.cci.aiw.i2b2etl;

import java.util.logging.Logger;

/**
 *
 * @author Andrew Post
 */
public class I2b2ETLUtil {
    private static class LazyLoggerHolder {

        private static Logger instance = 
                Logger.getLogger(I2b2ETLUtil.class.getPackage().getName());
    }
    
    /**
     * Gets the logger for this package.
     * 
     * @return a {@link Logger} object.
     */
    static Logger logger() {
        return LazyLoggerHolder.instance;
    }
}
