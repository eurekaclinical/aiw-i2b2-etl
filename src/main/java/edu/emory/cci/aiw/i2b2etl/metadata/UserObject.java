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

import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import javax.swing.tree.TreeNode;

public class UserObject {
    
    private static final String DEFAULT_FACT_TABLE_COLUMN = "concept_cd";
    private static final String DEFAULT_TABLE_NAME = "concept_dimension";
    private static final String DEFAULT_COLUMN_NAME = "concept_path";

    //	
    //	this class should eventually be split up into
    //	subclasses...  made polymorphic.
    //	
    //		by id:
    //
    //	/_Medication_/MED:medications/MED:mul/MED:(LME169) immunologic agents/MED:(LME173) interferons/MED:interferon beta-1b/MED:CDW:Extavia 0.3 mg subcutaneous injection
    //	/CPTCode/CPT:Level 1: 99201-99499/CPT:Level 2: 99374-99380/CPT:99380
    //	/_ICD9 Diagnostic Codes_/ICD9:Diagnoses/ICD9:678-679/ICD9:679/ICD9:679.1/ICD9:679.11
    //	/_ICD9 Procedure Codes_/ICD9:Procedures/ICD9:01-05/ICD9:01/ICD9:01.5/ICD9:01.59
    //	/_Laboratory Tests_/LAB:LabTest/LAB:Blood Gases/LAB:9915007
    //	/VitalSign/Temperature/TemperatureCore
    private final ConceptId id;
    private final Concept concept;
    private String displayName;
    private boolean inDataSource;
    private String conceptCode;		//	concept_dimension.concept_cd   &&   ontology.c_basecode
    private DataType dataType;
    private ValueTypeCode valueTypeCode;
    //	ontology.c_fullname
    //		AND
    //	ontology.c_dimcode
    //
    //  private String basecode = "";
    //	this depends on the species of ontologyNode (DEM|AGE,DEM|SEX,ICD9xyz,etc.)
    //	DEM|x:yz
    //	ICD9:123.4
    //	
    private String sourceSystemId;
    private boolean derived;
    private boolean inUse;
    private String conceptCodePrefix;
    private String dimCode;
    private boolean copy;
    private ArrayList<String> hierarchyPaths;
    private String appliedPath;

	// contains, eg, <ValueMetadata><loinc>...</loinc><ValueMetadata> to tell
	// i2b2 that this concept has numerical values
	private String metadataXml;
    private String factTableColumn;
    private String tableName;
    private String columnName;
    private ConceptOperator operator;

    UserObject(ConceptId id, String conceptCodePrefix, Concept concept, Metadata metadata) throws InvalidConceptCodeException {
        assert id != null : "id cannot be null";
        this.id = id;
        this.concept = concept;
        if (conceptCodePrefix != null) {
            OverriddenConceptCodeBuilder ccBuilder =
                    new OverriddenConceptCodeBuilder(metadata);
            ccBuilder.setId(conceptCodePrefix);
            ccBuilder.setValue(id.getValue());
            this.conceptCode = ccBuilder.build();
        } else {
            this.conceptCode = id.toConceptCode();
        }
        this.valueTypeCode = ValueTypeCode.UNSPECIFIED;
        this.conceptCodePrefix = conceptCodePrefix;
        this.appliedPath = "@";  //this field is mandatory in i2b2 1.7. assigning the default value
        this.factTableColumn = DEFAULT_FACT_TABLE_COLUMN;
        this.tableName = DEFAULT_TABLE_NAME;
        this.columnName = DEFAULT_COLUMN_NAME;
        this.operator = defaultOperator();
    }
    
    UserObject(UserObject usrObj, Concept concept) {
        this.id = usrObj.id;
        this.concept = concept;
        this.displayName = usrObj.displayName;
        this.inDataSource = usrObj.inDataSource;
        this.conceptCode = usrObj.conceptCode;
        this.dataType = usrObj.dataType;
        this.valueTypeCode = usrObj.valueTypeCode;
        this.sourceSystemId = usrObj.sourceSystemId;
        this.derived = usrObj.derived;
        this.inUse = usrObj.inUse;
        this.conceptCodePrefix = usrObj.conceptCodePrefix;
        this.copy = true;
        this.appliedPath = usrObj.appliedPath;
        this.factTableColumn = usrObj.factTableColumn;
        this.tableName = usrObj.tableName;
        this.columnName = usrObj.columnName;
        this.operator = usrObj.operator;
    }
    
    public String getConceptCodePrefix() {
        return this.conceptCodePrefix;
    }

    public ConceptId getId() {
        return id;
    }

    public String getDisplayName() {
        if (displayName == null || displayName.trim().length() == 0) {
            return getConceptCode();
        } else {
            return displayName;
        }
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    /**
     * Gets whether this concept should be queried with = or LIKE.
     * @return <code>true</code> if this concept should be queried with 
     * <code>=</code>, <code>false</code> if this concept should be queried
     * with <code>LIKE</code>.
     */
    public boolean isInDataSource() {
        return inDataSource;
    }

    /**
     * Sets whether this concept should be queried with = or LIKE.
     * @param b <code>true</code> to query for this concept with <code>=</code>,
     * <code>false</code> to query for it with <code>LIKE</code>.
     */
    public void setInDataSource(boolean inDataSource) {
        this.inDataSource = inDataSource;
    }

    public String getConceptCode() {
        return this.conceptCode;
    }

    public String getSourceSystemId() {
        return this.sourceSystemId;
    }

    public void setSourceSystemId(String sourceSystemId) {
        sourceSystemId = CodeUtil.truncateCodeStringIfNeeded(sourceSystemId);
        this.sourceSystemId = sourceSystemId;
    }
    
    public boolean isInUse() {
        return this.inUse;
    }
    
    public void setInUse(boolean inUse) {
        this.inUse = inUse;
    }

    public boolean isDerived() {
        return this.derived;
    }

    public void setDerived(boolean derived) {
        this.derived = derived;
    }

    public Concept getParent() {
        return (Concept) this.concept.getParent();
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public void setValueTypeCode(ValueTypeCode valueTypeCode) {
        if (valueTypeCode == null) {
            valueTypeCode = ValueTypeCode.UNSPECIFIED;
        }
        this.valueTypeCode = valueTypeCode;
    }

    public ValueTypeCode getValueTypeCode() {
        return this.valueTypeCode;
    }
    
    public void setOperator(ConceptOperator operator) {
        if (operator == null) {
            this.operator = defaultOperator();
        } else {
            this.operator = operator;
        }
    }

    /**
     * Returns which operator will be used to query this concept (
     * <code>EQUAL</code> or <code>LIKE</code>). To work around an i2b2 1.7 
     * bug, we force concepts with a dimcode containing a <code>_</code> to be 
     * queried with <code>LIKE</code>.
     * 
     * @return a concept operator.
     */
    public ConceptOperator getOperator() {
        return this.operator;
    }
    
    private ConceptOperator defaultOperator() {
        if (isInDataSource() && !getDimCode().contains("_")) {
            return ConceptOperator.EQUAL;
        } else {
            return ConceptOperator.LIKE;
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    void setDimCode(String dimCode) {
        this.dimCode = dimCode;
    }
    
    public String getI2B2Path() {
        TreeNode[] tna = this.concept.getPath();
        StringBuilder path = new StringBuilder();
        for (TreeNode tn : tna) {
            path.append('\\');
            path.append(((Concept) tn).getConceptCode());
        }
        path.append('\\');
        return path.toString();
    }
    
    String getDimCode() {
        String result = this.dimCode;
        if (result == null) {
            result = getI2B2Path();
        }
        return result;
    }

    public String getMetadataXml() {
            return metadataXml;
    }

    public void setMetadataXml(String metadataXml) {
            this.metadataXml = metadataXml;
    }

    public ArrayList<String> getHierarchyPaths() {
        return this.hierarchyPaths;
    }

    public void setHierarchyPath(String path) {
        if (this.hierarchyPaths == null) {
            hierarchyPaths = new ArrayList<String>();
        }
        if (!this.hierarchyPaths.contains(path)) {
            hierarchyPaths.add(path);
        }
    }

    public String getAppliedPath() {
        return this.appliedPath;
    }

    void setFactTableColumn(String factTableColumn) {
        if (factTableColumn == null) {
            this.factTableColumn = DEFAULT_FACT_TABLE_COLUMN;
        } else {
            this.factTableColumn = factTableColumn;
        }
    }

    String getFactTableColumn() {
        return this.factTableColumn;
    }

    void setTableName(String tableName) {
        if (tableName == null) {
            this.tableName = DEFAULT_TABLE_NAME;
        } else {
            this.tableName = tableName;
        }
    }

    String getTableName() {
        return this.tableName;
    }

    void setColumnName(String columnName) {
        if (columnName == null) {
            this.columnName = DEFAULT_COLUMN_NAME;
        } else {
            this.columnName = columnName;
        }
    }

    String getColumnName() {
        return this.columnName;
    }

}
