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
import org.protempa.ValueSet;
import org.protempa.ValueSet.ValueSetElement;
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
    private StringBuilder charBuffer;
    private ValueType valueType = ValueType.VALUE;
    private ValueSet valueSet;
    private List<ValueSetElement> valueSetElements;
    private String valueSetElementDescription;
    private String unitsOfMeasure;
    private SAXParseException exception;

    public CMetadataXmlParser() {
        this.valueSetElements = new ArrayList<>();
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

        switch (localName) {
            case "DataType":
            case "Val":
//            case "NormalUnits":
//                this.tag = localName;
        }

        switch (localName) {
            case "Val":
                if (this.conceptBaseCode != null) {
                    this.valueSetElementDescription = atts.getValue("description");
                }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (this.tag != null) {
            charBuffer.append(ch);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (localName) {
            case "DataType":
                switch (this.charBuffer.toString()) {
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
                    this.valueSetElements.add(new ValueSetElement(NominalValue.getInstance(this.charBuffer.toString()), this.valueSetElementDescription, null));
                    this.valueSetElementDescription = null;
                }
                break;
//            case "NormalUnits":
//                this.unitsOfMeasure = this.charBuffer.toString();
//                break;
        }
        if (this.charBuffer != null) {
            this.charBuffer.setLength(0);
        }
        this.tag = null;
    }

    @Override
    public void endDocument() throws SAXException {
        if (!this.valueSetElements.isEmpty()) {
            this.valueSet = new ValueSet(
                    this.conceptBaseCode,
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
