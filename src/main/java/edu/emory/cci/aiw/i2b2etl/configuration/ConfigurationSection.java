package edu.emory.cci.aiw.i2b2etl.configuration;

import org.w3c.dom.*;

/**
 *
 * @author Andrew Post
 */
abstract class ConfigurationSection {
    
    protected abstract String getNodeName();
    
    protected abstract void put(NamedNodeMap attributes) throws ConfigurationReadException;
    
    void load(Element elm) throws ConfigurationReadException {
        NodeList nL = elm.getChildNodes();
        for (int i = 0; i < nL.getLength(); i++) {
            Node section = nL.item(i);
            if (section.getNodeType() == Node.ELEMENT_NODE) {
                if (section.getNodeName().equals(getNodeName())) {
                    put(section.getAttributes());
                }
            }
        }
    }
    
    protected static String readAttribute(NamedNodeMap nnm, String namedItem, boolean required) throws ConfigurationReadException {
        Attr attr = (Attr) nnm.getNamedItem(namedItem);
        String val;
        if (attr == null) {
            val = null;
        } else {
            val = attr.getValue();
        }
        if (required && (val == null)) {
            throw new ConfigurationReadException("bad " + namedItem + " definition in configuration file");
        }
        return val;
    }
}
