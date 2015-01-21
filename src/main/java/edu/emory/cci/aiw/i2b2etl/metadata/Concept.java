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

import edu.emory.cci.aiw.i2b2etl.table.Record;
import javax.swing.tree.DefaultMutableTreeNode;
import java.util.ArrayList;
import java.util.Date;

public final class Concept extends DefaultMutableTreeNode implements Record {

    private static final long serialVersionUID = 350127486830611865L;
    private final UserObject usrObj;

    //	ICD9:429.2			diagnoses
    //	ICD9:07.02			procedures
    //	LCS-I2B2:blah		providers
    //	LOINC:1997-6		labtests
    //	NDC:00015733999		drugs
    //	DEM|xyz:
    /*  CREATE TABLE I2B2 (
    "C_HLEVEL"                              NUMBER(22,0)    NOT NULL,
    "C_FULLNAME"                    VARCHAR2(700)   NOT NULL,
    "C_NAME"                                VARCHAR2(2000)  NOT NULL,
    "C_SYNONYM_CD"                  CHAR(1) NOT NULL,
    "C_VISUALATTRIBUTES"    CHAR(3) NOT NULL,
    "C_TOTALNUM"                    NUMBER(22,0)    NULL,
    "C_BASECODE"                    VARCHAR2(50)    NULL,
    "C_METADATAXML"                 CLOB    NULL,
    "C_FACTTABLECOLUMN"             VARCHAR2(50)    NOT NULL,
    "C_TABLENAME"                   VARCHAR2(50)    NOT NULL,
    "C_COLUMNNAME"                  VARCHAR2(50)    NOT NULL,
    "C_COLUMNDATATYPE"              VARCHAR2(50)    NOT NULL,
    "C_OPERATOR"                    VARCHAR2(10)    NOT NULL,
    "C_DIMCODE"                             VARCHAR2(700)   NOT NULL,
    "C_COMMENT"                             CLOB    NULL,
    "C_TOOLTIP"                             VARCHAR2(900)   NULL,
    "M_APPLIED_PATH"                VARCHAR2(700)   NOT NULL,
    "UPDATE_DATE"                   DATE    NOT NULL,
    "DOWNLOAD_DATE"                 DATE    NULL,
    "IMPORT_DATE"                   DATE    NULL,
    "SOURCESYSTEM_CD"               VARCHAR2(50)    NULL,
    "VALUETYPE_CD"                  VARCHAR2(50)    NULL,
    "M_EXCLUSION_CD"                        VARCHAR2(25) NULL,
    "C_PATH"                                VARCHAR2(700)   NULL,
    "C_SYMBOL"                              VARCHAR2(50)    NULL
    )*/
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

    /**
     * Gets whether this concept should be queried with = or LIKE.
     * @return <code>true</code> if this concept should be queried with 
     * <code>=</code>, <code>false</code> if this concept should be queried
     * with <code>LIKE</code>.
     */
    public boolean isInDataSource() {
        return usrObj.isInDataSource();
    }

    /**
     * Sets whether this concept should be queried with = or LIKE.
     * @param b <code>true</code> to query for this concept with <code>=</code>,
     * <code>false</code> to query for it with <code>LIKE</code>.
     */
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
    
    public String getFullName() {
        return usrObj.getFullName();
    }
    
    public void setDimCode(String dimCode) {
        usrObj.setDimCode(dimCode);
    }
    
    public String getDimCode() {
        return this.usrObj.getDimCode();
    }
    
    public void setCVisualAttributes(String attrs) {
        usrObj.setCVisualAttributes(attrs);
    }

    public String getCVisualAttributes() {
        String attrs = usrObj.getCVisualAttributes();
        if (attrs == null) {
            return isLeaf() ? "LAE" : "FAE";
        } else {
            return attrs;
        }
    }
    
    public String getSymbol() {
        return usrObj.getSymbol();
    }
    
    public String getCPath() {
        return usrObj.getCPath();
    }
    
    public String getToolTip() {
        return usrObj.getToolTip();
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
    
    public void setOperator(ConceptOperator operator) {
        usrObj.setOperator(operator);
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
    
    @Override
    public String toString() {
        return getConceptCode();
    }

    public ArrayList<String> getHierarchyPaths() {
        return usrObj.getHierarchyPaths();
    }

    public void setHierarchyPath(String path) {
        usrObj.setHierarchyPath(path);
    }

    public String getAppliedPath() {
        return usrObj.getAppliedPath();
    }
    
    public void setFactTableColumn(String factTableColumn) {
        usrObj.setFactTableColumn(factTableColumn);
    }

    public String getFactTableColumn() {
        return usrObj.getFactTableColumn();
    }
    
    public void setTableName(String tableName) {
        usrObj.setTableName(tableName);
    }

    public String getTableName() {
        return usrObj.getTableName();
    }
    
    public void setColumnName(String columnName) {
        usrObj.setColumnName(columnName);
    }

    public String getColumnName() {
        return usrObj.getColumnName();
    }
    
    public void setComment(String comment) {
        usrObj.setComment(comment);
    }
    
    public String getComment() {
        return usrObj.getComment();
    }
    
    public void setDownloaded(Date downloaded) {
        usrObj.setDownloaded(downloaded);
    }
    
    public Date getDownloaded() {
        return usrObj.getDownloaded();
    }

    @Override
    public boolean isRejected() {
        return false;
    }
    
}
