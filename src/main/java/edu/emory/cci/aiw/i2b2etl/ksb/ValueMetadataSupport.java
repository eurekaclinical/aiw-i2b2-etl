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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.protempa.KnowledgeSourceReadException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 *
 * @author Andrew Post
 */
class ValueMetadataSupport {
    
    private final SAXParser saxParser;

    ValueMetadataSupport() throws KnowledgeSourceReadException {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        try {
            this.saxParser = spf.newSAXParser();
        } catch (ParserConfigurationException | SAXException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
    }

    XMLReader init(CMetadataXmlParser valueMetadataParser) throws KnowledgeSourceReadException {
        XMLReader xmlReader;
        try {
            xmlReader = saxParser.getXMLReader();
        } catch (SAXException ex) {
            throw new KnowledgeSourceReadException(ex);
        }
        xmlReader.setContentHandler(valueMetadataParser);
        return xmlReader;
    }
    
    void parse(XMLReader xmlReader, String clob) throws KnowledgeSourceReadException {
        if (clob != null) {
            try (Reader r = new StringReader(clob)) {
                xmlReader.parse(new InputSource(r));
            } catch (SAXException | IOException sqle) {
                throw new KnowledgeSourceReadException(sqle);
            }
        }
    }

}
