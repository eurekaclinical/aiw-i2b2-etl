package edu.emory.cci.aiw.i2b2etl.dsb;

/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2014 Emory University
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

import au.com.bytecode.opencsv.CSVReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import org.protempa.backend.dsb.relationaldb.ColumnSpec;

/**
 *
 * @author Andrew Post
 */
public abstract class CSVMapper implements Mapper {

    /**
     * Closes the supplied reader.
     * 
     * @param reader
     * @return
     * @throws IOException 
     */
    protected ColumnSpec.KnowledgeSourceIdToSqlCode[] propertyNameOrPropIdToSqlCodeArray(Reader reader) throws IOException {
        List<ColumnSpec.KnowledgeSourceIdToSqlCode> cvs = new ArrayList<>(
                1000);
        try (CSVReader r = new CSVReader(reader, '\t')) {
            String[] cols;
            int i = 1;
            while ((cols = r.readNext()) != null) {
                if (cols.length > 2) {
                    throw new AssertionError("Invalid mapping in line " + i + ": mapping has length " + cols.length);
                } else if (cols.length == 1) {
                    cvs.add(new ColumnSpec.KnowledgeSourceIdToSqlCode(cols[0],
                        ""));
                } else if (cols.length == 2) {
                    cvs.add(new ColumnSpec.KnowledgeSourceIdToSqlCode(cols[0],
                        cols[1]));
                }
                i++;
            }
        }
        return cvs
                .toArray(new ColumnSpec.KnowledgeSourceIdToSqlCode[cvs.size()]);
    }

    /**
     * Closes the supplied reader.
     * 
     * @param reader
     * @param colNum
     * @return
     * @throws IOException 
     */
    protected String[] readCodes(Reader reader, int colNum) throws IOException {
        List<String> codes = new ArrayList<>();
        try (CSVReader r = new CSVReader(reader, '\t')) {
            String[] cols;
            int i = 1;
            while ((cols = r.readNext()) != null) {
                if (cols.length > 0) {
                    if (cols.length < colNum) {
                        throw new AssertionError("Invalid mapping in line " + i + ": mapping has length " + cols.length);
                    }
                    codes.add(cols[colNum].trim());
                }
                i++;
            }
        }
        return codes.toArray(new String[codes.size()]);
    }
    
}
