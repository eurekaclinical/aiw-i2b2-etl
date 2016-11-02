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
import java.util.List;
import org.protempa.KnowledgeSourceReadException;
import org.protempa.PropertyDefinition;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.ValueType;
import org.protempa.valueset.ValueSet;
import org.protempa.valueset.ValueSetElement;
import org.xml.sax.SAXParseException;

/**
 *
 * @author Andrew Post
 */
final class PropertyDefinitionFactory {

    private final ValueMetadataParser valueMetadataParser;

    PropertyDefinitionFactory() throws KnowledgeSourceReadException {
        this.valueMetadataParser = new ValueMetadataParser();
    }

    ValueSet getValueSetInstance(String valueSetId, List<String> nominalValues) {
        ValueSetElement[] elts = new ValueSetElement[nominalValues.size()];
        for (int i = 0, n = nominalValues.size(); i < n; i++) {
            elts[i] = new ValueSetElement(NominalValue.getInstance(nominalValues.get(i)));
        }
        return new ValueSet(valueSetId, null, elts, null);
    }

    ValueSet getValueSetInstance(String valueSetId, String clob) throws ParseException, KnowledgeSourceReadException, SAXParseException {
        this.valueMetadataParser.init();
        this.valueMetadataParser.parseValueSetId(valueSetId);
        this.valueMetadataParser.parse(clob);
        return this.valueMetadataParser.getValueSet();
    }

    PropertyDefinition getPropertyDefinitionInstance(String clob, String declaringConceptSymbol, String symbol, String conceptSymbol, String name) throws KnowledgeSourceReadException, SAXParseException {
        ValueType valueType;
        this.valueMetadataParser.init();
        this.valueMetadataParser.setDeclaringPropId(declaringConceptSymbol);
        this.valueMetadataParser.setConceptBaseCode(symbol);
        if (clob != null) {
            this.valueMetadataParser.parse(clob);
            valueType = this.valueMetadataParser.getValueType();
        } else {
            valueType = ValueType.NOMINALVALUE;
        }
        return new PropertyDefinition(conceptSymbol, symbol, name, valueType, valueMetadataParser.getValueSetId(), declaringConceptSymbol);
    }
}
