package edu.emory.cci.aiw.i2b2etl.cli;

import java.io.File;
import java.io.IOException;
import org.arp.javautil.io.IOUtil;

/**
 *
 * @author Andrew Post
 */
class I2b2ETLConfAsFile {
    private final File confXML;

    public I2b2ETLConfAsFile() throws IOException {
        this.confXML = IOUtil.resourceToFile("/conf.xml", "conf", null);
    }
    
    File getFile() {
        return this.confXML;
    }
}
