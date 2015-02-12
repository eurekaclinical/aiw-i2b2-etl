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
package edu.emory.cci.aiw.i2b2etl.dest;

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
