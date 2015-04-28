package edu.emory.cci.aiw.i2b2etl.ksb;

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
import java.io.IOException;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;

/**
 *
 * @author Andrew Post
 */
final class ValueSetSupport {

    private static final char DELIMITER = '^';
    private static final CSVFormat CSV_FORMAT = 
            CSVFormat.newFormat(DELIMITER)
                    .withQuote('"')
                    .withQuoteMode(QuoteMode.ALL)
                    .withEscape('\\');

    private String declaringPropId;
    private String propertyName;
    
    boolean isValid() {
        return this.declaringPropId != null && this.propertyName != null;
    }

    String getDeclaringPropId() {
        return declaringPropId;
    }

    void setDeclaringPropId(String declaringPropId) {
        this.declaringPropId = declaringPropId;
    }

    String getPropertyName() {
        return propertyName;
    }

    void setPropertyName(String propertyName) {
        this.propertyName = propertyName;
    }

    void parseId(String id) {
        try (CSVParser csvParser = CSVParser.parse(id, CSV_FORMAT)) {
            List<CSVRecord> records = csvParser.getRecords();
            if (records.size() != 1) {
                return;
            }
            CSVRecord record = records.get(0);
            if (record.size() != 2) {
                return;
            }
            this.declaringPropId = record.get(0);
            this.propertyName = record.get(1);
        } catch (IOException invalid) {
        }
    }

    String getId() {
        if (this.declaringPropId == null) {
            throw new IllegalStateException("declaringPropId cannot be null");
        }
        if (this.propertyName == null) {
            throw new IllegalStateException("propertyName cannot be null");
        }
        return CSV_FORMAT.format(this.declaringPropId, this.propertyName);
    }
}
