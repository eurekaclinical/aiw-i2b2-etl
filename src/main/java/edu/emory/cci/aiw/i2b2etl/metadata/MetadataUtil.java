package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import java.util.logging.Logger;

/**
 *
 * @author Andrew Post
 */
public class MetadataUtil {

    private static class LazyLoggerHolder {

        private static Logger instance =
                Logger.getLogger(MetadataUtil.class.getPackage().getName());
    }

    /**
     * Gets the logger for this package.
     *
     * @return a {@link Logger} object.
     */
    static Logger logger() {
        return LazyLoggerHolder.instance;
    }

    public static String toSourceSystemCode(String sourceIdString) {
        if (sourceIdString.length() > CodeUtil.CODE_LENGTH) {
            return sourceIdString.substring(0, CodeUtil.CODE_LENGTH);
        } else {
            return sourceIdString;
        }
    }
    
    static final String DEFAULT_CONCEPT_ID_PREFIX_INTERNAL = "i2b2INTERNAL";
}
