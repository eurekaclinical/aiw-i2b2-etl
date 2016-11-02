package edu.emory.cci.aiw.i2b2etl.ksb;

/*-
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 - 2016 Emory University
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
import java.text.ParseException;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.proposition.value.ValueType;
import org.protempa.valueset.ValueSet;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Andrew Post
 */
class ValueMetadataParser {

    private final ValueMetadataSupport valueMetadataSupport;
    private CMetadataXmlParser valueMetadataParser;
    private XMLReader xmlReader;
    private ValueSetSupport valueSetSupport;

    ValueMetadataParser() throws KnowledgeSourceReadException {
        this.valueMetadataSupport = new ValueMetadataSupport();
    }

    void init() throws KnowledgeSourceReadException {
        this.valueSetSupport = new ValueSetSupport();
    }

    void parseValueSetId(String valueSetId) throws ParseException {
        this.valueSetSupport.parseId(valueSetId);
    }

    String getDeclaringPropId() {
        return this.valueSetSupport.getDeclaringPropId();
    }

    void setDeclaringPropId(String declaringPropId) {
        this.valueSetSupport.setDeclaringPropId(declaringPropId);
    }

    void setConceptBaseCode(String conceptBaseCode) {
        this.valueSetSupport.setPropertyName(conceptBaseCode);
    }

    String getConceptBaseCode() {
        return this.valueSetSupport.getPropertyName();
    }

    void parse(String clob) throws KnowledgeSourceReadException, SAXParseException {
        if (this.valueMetadataParser == null) {
            this.valueMetadataParser = new CMetadataXmlParser();
            this.xmlReader = valueMetadataSupport.init(this.valueMetadataParser);
        }
        this.valueMetadataParser.setDeclaringPropId(this.valueSetSupport.getDeclaringPropId());
        this.valueMetadataParser.setConceptBaseCode(this.valueSetSupport.getPropertyName());
        this.valueMetadataSupport.parse(xmlReader, clob);
        SAXParseException exception = valueMetadataParser.getException();
        if (exception != null) {
            throw exception;
        }
    }

    ValueType getValueType() {
        if (this.valueMetadataParser == null) {
            return null;
        } else {
            return this.valueMetadataParser.getValueType();
        }
    }

    ValueSet getValueSet() {
        if (this.valueMetadataParser == null) {
            return null;
        } else {
            return this.valueMetadataParser.getValueSet();
        }
    }

    String getValueSetId() {
        return this.valueSetSupport.getId();
    }

}
