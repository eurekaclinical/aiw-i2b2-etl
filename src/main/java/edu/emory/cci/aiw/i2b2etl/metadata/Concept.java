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
package edu.emory.cci.aiw.i2b2etl.metadata;

import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeNode;

public final class Concept extends DefaultMutableTreeNode {

    private static final long serialVersionUID = 350127486830611865L;
    private final UserObject usrObj;

    //	ICD9:429.2			diagnoses
    //	ICD9:07.02			procedures
    //	LCS-I2B2:blah		providers
    //	LOINC:1997-6		labtests
    //	NDC:00015733999		drugs
    //	DEM|xyz:
    //  CREATE TABLE  "I2B2"
    //  (
    //	"C_HLEVEL"					NUMBER(22,0) NOT NULL ENABLE, 
    // 	"C_FULLNAME"				VARCHAR2(700) NOT NULL ENABLE, 
    // 	"C_NAME"					VARCHAR2(2000) NOT NULL ENABLE, 
    // 	"C_SYNONYM_CD" 				CHAR(1) NOT NULL ENABLE, 
    // 	"C_VISUALATTRIBUTES"		CHAR(3) NOT NULL ENABLE, 
    // 	"C_TOTALNUM"				NUMBER(22,0), 
    // 	"C_BASECODE"				VARCHAR2(50), 
    // 	"C_METADATAXML"				CLOB, 
    // 	"C_FACTTABLECOLUMN"			VARCHAR2(50) NOT NULL ENABLE, 
    // 	"C_TABLENAME"				VARCHAR2(50) NOT NULL ENABLE, 
    // 	"C_COLUMNNAME"				VARCHAR2(50) NOT NULL ENABLE, 
    // 	"C_COLUMNDATATYPE"			VARCHAR2(50) NOT NULL ENABLE, 
    // 	"C_OPERATOR"				VARCHAR2(10) NOT NULL ENABLE, 
    // 	"C_DIMCODE"					VARCHAR2(700) NOT NULL ENABLE, 
    // 	"C_COMMENT"					CLOB, 
    // 	"C_TOOLTIP"					VARCHAR2(900), 
    // 	"UPDATE_DATE"				DATE NOT NULL ENABLE, 
    // 	"DOWNLOAD_DATE"				DATE, 
    // 	"IMPORT_DATE"				DATE, 
    // 	"SOURCESYSTEM_CD"			VARCHAR2(50), 
    // 	"VALUETYPE_CD"				VARCHAR2(50)
    // 	)
    public Concept(ConceptId id, String conceptCodePrefix, Metadata metadata) throws InvalidConceptCodeException {
        this.usrObj = new UserObject(id, conceptCodePrefix, this, metadata);
    }
    
    public Concept(Concept concept, Metadata metadata) throws InvalidConceptCodeException {
        this.usrObj = new UserObject(concept.usrObj, this);
    }
    
    public String getConceptCodePrefix() {
        return usrObj.getConceptCodePrefix();
    }

    public ConceptId getId() {
        return usrObj.getId();
    }

    @Override
    public Object getUserObject() {
        return usrObj.getId();
    }

    public boolean isDerived() {
        return usrObj.isDerived();
    }

    public void setDerived(boolean derived) {
        usrObj.setDerived(derived);
    }

    public boolean isInDataSource() {
        return usrObj.isInDataSource();
    }

    public void setInDataSource(boolean b) {
        usrObj.setInDataSource(b);
    }
    
    public boolean isInUse() {
        return usrObj.isInUse();
    }
    
    public void setInUse(boolean inUse) {
        usrObj.setInUse(inUse);
    }

    public String getSourceSystemCode() {
        return usrObj.getSourceSystemId();
    }

    public void setSourceSystemCode(String sourceSystemId) {
        usrObj.setSourceSystemId(sourceSystemId);
    }

    public String getI2B2Path() {
        TreeNode[] tna = this.getPath();
        StringBuilder path = new StringBuilder();
        for (TreeNode tn : tna) {
            path.append('\\');
            path.append(((Concept) tn).getConceptCode());
        }
        path.append('\\');
        return path.toString();
    }
    
    public void setDimCode(String dimCode) {
        usrObj.setDimCode(dimCode);
    }
    
    public String getDimCode() {
        String result = usrObj.getDimCode();
        if (result == null) {
            result = getI2B2Path();
        }
        return result;
    }

    public String getCVisualAttributes() {
        return isLeaf() ? "LAE" : "FAE";
    }

    public String getConceptCode() {
        return usrObj.getConceptCode();
    }

    public String getDisplayName() {
        return usrObj.getDisplayName();
    }

    public void setDisplayName(String displayName) {
        usrObj.setDisplayName(displayName);
    }
    
    public ConceptOperator getOperator() {
        return usrObj.getOperator();
    }
    
    public void setDataType(DataType dataType) {
        usrObj.setDataType(dataType);
    }
    
    public DataType getDataType() {
        return usrObj.getDataType();
    }
    
    public void setValueTypeCode(ValueTypeCode valueTypeCode) {
        usrObj.setValueTypeCode(valueTypeCode);
    }
    
    public ValueTypeCode getValueTypeCode() {
        return usrObj.getValueTypeCode();
    }

	public void setMetadataXml(String metadataXml) {
		usrObj.setMetadataXml(metadataXml);
	}

	public String getMetadataXml() {
		return usrObj.getMetadataXml();
	}
    
    public boolean isCopy() {
        return usrObj.isCopy();
    }

    @Override
    public String toString() {
        return getConceptCode();
    }
}
