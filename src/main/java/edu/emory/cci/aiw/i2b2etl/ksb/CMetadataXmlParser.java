package edu.emory.cci.aiw.i2b2etl.ksb;

/*
 * #%L
 * Protempa i2b2 Knowledge Source Backend
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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.protempa.valueset.ValueSet;
import org.protempa.valueset.ValueSetElement;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.ValueType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Andrew Post
 */
class CMetadataXmlParser extends DefaultHandler {

    private static final Logger LOGGER = Logger.getLogger(CMetadataXmlParser.class.getName());
    private String conceptBaseCode;
    private String tag;
    private final StringBuilder charBuffer;
    private ValueType valueType;
    private ValueSet valueSet;
    private final List<ValueSetElement> valueSetElements;
    private String valueSetElementDescription;
    private String unitsOfMeasure;
    private SAXParseException exception;
    private String declaringPropId;

    CMetadataXmlParser() {
        this.valueSetElements = new ArrayList<>();
        this.charBuffer = new StringBuilder();
    }

    String getDeclaringPropId() {
        return declaringPropId;
    }

    void setDeclaringPropId(String declaringPropId) {
        this.declaringPropId = declaringPropId;
    }

    void setConceptBaseCode(String conceptBaseCode) {
        this.conceptBaseCode = conceptBaseCode;
    }

    String getConceptBaseCode() {
        return conceptBaseCode;
    }

    ValueType getValueType() {
        return valueType;
    }

    ValueSet getValueSet() {
        return valueSet;
    }

    String getUnitsOfMeasure() {
        return unitsOfMeasure;
    }

    SAXParseException getException() {
        return this.exception;
    }

    @Override
    public void startDocument() throws SAXException {
        this.valueType = ValueType.VALUE;
    }

    @Override
    public void startElement(String namespaceURI,
            String localName,
            String qName,
            Attributes atts)
            throws SAXException {
        
        switch (qName) {
            case "DataType":
            case "Val":
                this.tag = qName;
//            case "NormalUnits":
//                this.tag = localName;
        }

        switch (qName) {
            case "Val":
                if (this.conceptBaseCode != null) {
                    this.valueSetElementDescription = atts.getValue("description");
                }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (this.tag != null) {
            charBuffer.append(String.valueOf(ch, start, length));
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        try {
            switch (qName) {
                case "DataType":
                    switch (String.valueOf(this.charBuffer)) {
                        case "PosInteger":
                        case "Integer":
                        case "Float":
                        case "PosFloat":
                            this.valueType = ValueType.NUMERICALVALUE;
                            break;
                        case "Enum":
                        case "String":
                            this.valueType = ValueType.NOMINALVALUE;
                            break;
                        default:
                            this.valueType = ValueType.VALUE;
                    }
                    break;
                case "Val":
                    if (this.conceptBaseCode != null) {
                        this.valueSetElements.add(new ValueSetElement(NominalValue.getInstance(this.charBuffer.toString()), this.valueSetElementDescription));
                        this.valueSetElementDescription = null;
                    }
                    break;
//            case "NormalUnits":
//                this.unitsOfMeasure = this.charBuffer.toString();
//                break;//            case "NormalUnits":
//                this.unitsOfMeasure = this.charBuffer.toString();
//                break;
            }
        } finally {
            this.charBuffer.setLength(0);
            this.tag = null;
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (!this.valueSetElements.isEmpty()) {
            ValueSetSupport vsSupport = new ValueSetSupport();
            vsSupport.setDeclaringPropId(this.declaringPropId);
            vsSupport.setPropertyName(this.conceptBaseCode);
            this.valueSet = new ValueSet(
                    vsSupport.getId(), null,
                    this.valueSetElements.toArray(new ValueSetElement[this.valueSetElements.size()]),
                    null
            );
            this.valueSetElements.clear();
        }
    }

    @Override
    public void fatalError(SAXParseException e) throws SAXException {
        this.exception = e;
    }

    @Override
    public void error(SAXParseException e) throws SAXException {
        LOGGER.log(Level.SEVERE, "Recoverable error while parsing c_metadataxml", e);
    }

    @Override
    public void warning(SAXParseException e) throws SAXException {
        LOGGER.log(Level.WARNING, "Warning while parsing c_metadataxml", e);
    }

}
