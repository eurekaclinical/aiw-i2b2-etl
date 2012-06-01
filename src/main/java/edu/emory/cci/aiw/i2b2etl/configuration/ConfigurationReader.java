package edu.emory.cci.aiw.i2b2etl.configuration;

import java.io.File;
import java.io.IOException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Andrew Post
 */
public final class ConfigurationReader {
    private final DictionarySection dictionary;
    private final DatabaseSection database;
    private final ConceptsSection concepts;
    private final DataSection data;
    private final File conf;
    
    public ConfigurationReader(File confFile) {
        this.conf = confFile;
        this.dictionary = new DictionarySection();
        this.database = new DatabaseSection();
        this.concepts = new ConceptsSection();
        this.data = new DataSection();
    }
    
    public void read() throws ConfigurationReadException {
        try {
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = db.parse(conf);
            Element eRoot = doc.getDocumentElement();
            NodeList nL = eRoot.getChildNodes();
            for (int i = 0; i < nL.getLength(); i++) {

                Node node = nL.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    if (node.getNodeName().equals("dictionary")) {
                        dictionary.load((Element) node);
                    } else if (node.getNodeName().equals("database")) {
                        database.load((Element) node);
                    } else if (node.getNodeName().equals("concepts")) {
                        concepts.load((Element) node);
                    } else if (node.getNodeName().equals("data")) {
                        data.load((Element) node);
                    }
                }
            }
        } catch (SAXException ex) {
            throw new ConfigurationReadException("Could not read configuration file " + this.conf.getAbsolutePath(), ex);
        } catch (IOException ex) {
            throw new ConfigurationReadException("Could not read configuration file " + this.conf.getAbsolutePath(), ex);
        } catch (ParserConfigurationException ex) {
            throw new ConfigurationReadException("Could not read configuration file " + this.conf.getAbsolutePath(), ex);
        }
    }
    
    public DictionarySection getDictionarySection() {
        return this.dictionary;
    }
    
    public DatabaseSection getDatabaseSection() {
        return this.database;
    }
    
    public ConceptsSection getConceptsSection() {
        return this.concepts;
    }
    
    public DataSection getDataSection() {
        return this.data;
    }
    
    
}
