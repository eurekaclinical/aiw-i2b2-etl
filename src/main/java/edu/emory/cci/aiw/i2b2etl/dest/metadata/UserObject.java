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
package edu.emory.cci.aiw.i2b2etl.dest.metadata;

import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.InvalidConceptCodeException;
import edu.emory.cci.aiw.i2b2etl.dest.metadata.conceptid.ConceptId;
import edu.emory.cci.aiw.etl.util.CodeUtil;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;

public class UserObject {
    
    private static final String DEFAULT_FACT_TABLE_COLUMN = "concept_cd";
    private static final String DEFAULT_TABLE_NAME = "concept_dimension";
    private static final String DEFAULT_COLUMN_NAME = "concept_path";
    private static final DataType DEFAULT_DATA_TYPE = DataType.TEXT;
    private static final int TOOL_TIP_MAX_LENGTH = 900;
    private static final int DISPLAY_NAME_MAX_LENGTH = 2000;

    private final ConceptId id;
    private final Concept concept;
    private String displayName;
    private boolean inDataSource;
    private final String conceptCode;
    private DataType dataType;
    private ValueTypeCode valueTypeCode;
    private String sourceSystemId;
    private boolean derived;
    private boolean inUse;
    private final String conceptCodePrefix;
    private String dimCode;
    private Set<String> hierarchyPaths;
    private String appliedPath;
    private String metadataXml;
    private String factTableColumn;
    private String tableName;
    private String columnName;
    private ConceptOperator operator;
    private String cVisualAttributes;
    private String comment;
    
    private Date downloaded;
    private final PathSupport pathSupport;
    private String fullName;
    private String cPath;
    private String toolTip;
    private int level = -1;
    private SynonymCode synonymCode;
    private boolean alreadyLoaded;private boolean rejected;
    private List<String> rejectionReasons;
    private String symbol;
    private Date updated;
    
    UserObject(ConceptId id, String conceptCodePrefix, Concept concept, Metadata metadata) throws InvalidConceptCodeException {
        assert id != null : "id cannot be null";
        this.id = id;
        this.concept = concept;
        this.conceptCode = id.toConceptCode(conceptCodePrefix);
        this.valueTypeCode = ValueTypeCode.UNSPECIFIED;
        this.dataType = DEFAULT_DATA_TYPE;
        this.conceptCodePrefix = conceptCodePrefix;
        this.appliedPath = "@";  //this field is mandatory in i2b2 1.7. assigning the default value
        this.factTableColumn = DEFAULT_FACT_TABLE_COLUMN;
        this.tableName = DEFAULT_TABLE_NAME;
        this.columnName = DEFAULT_COLUMN_NAME;
        this.operator = defaultOperator();
        this.pathSupport = new PathSupport();
        this.pathSupport.setConcept(this.concept);
        this.synonymCode = SynonymCode.NOT_SYNONYM;
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
        } else if (displayName.length() <= DISPLAY_NAME_MAX_LENGTH) {
            return displayName;
        } else {
            return displayName.substring(0, DISPLAY_NAME_MAX_LENGTH);
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
        if (dataType == null) {
            this.dataType = DEFAULT_DATA_TYPE;
        } else {
            this.dataType = dataType;
        }
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public void setValueTypeCode(ValueTypeCode valueTypeCode) {
        if (valueTypeCode == null) {
            this.valueTypeCode = ValueTypeCode.UNSPECIFIED;
        } else {
            this.valueTypeCode = valueTypeCode;
        }
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

    void setDimCode(String dimCode) {
        this.dimCode = dimCode;
    }
    
    public String getSymbol() {
        return getConceptCode();
    }
    
    public String getCPath() {
        if (this.cPath != null) {
            return this.cPath;
        } else {
            return this.pathSupport.getCPath();
        }
    }
    
    public String getFullName() {
        if (this.fullName != null) {
            return this.fullName;
        } else {
            return this.pathSupport.getFullName();
        }
    }
    
    public String getToolTipInt() {
        if (this.toolTip != null) {
            return this.toolTip;
        } else {
            return this.pathSupport.getToolTip();
        }
    }
    
    public String getToolTip() {
        String toolTip = getToolTipInt();
        
        if (toolTip == null || toolTip.length() > TOOL_TIP_MAX_LENGTH) {
            toolTip = getDisplayName();
            if (toolTip != null && toolTip.length() > TOOL_TIP_MAX_LENGTH) {
                return toolTip.substring(0, TOOL_TIP_MAX_LENGTH);
            } else {
                return null;
            }
        } else {
            return toolTip;
        }
    }
    
    String getDimCode() {
        String result = this.dimCode;
        if (result == null) {
            result = getFullName();
        }
        if (result == null) {
            return "";
        } else {
            return result;
        }
    }

    public String getMetadataXml() {
            return metadataXml;
    }

    public void setMetadataXml(String metadataXml) {
            this.metadataXml = metadataXml;
    }

    public String[] getHierarchyPaths() {
        if (this.hierarchyPaths == null) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        } else {
            return this.hierarchyPaths.toArray(new String[this.hierarchyPaths.size()]);
        }
    }

    public void addHierarchyPath(String path) {
        if (this.hierarchyPaths == null) {
            this.hierarchyPaths = new HashSet<>();
        }
        this.hierarchyPaths.add(path);
    }
    
    public void setAppliedPath(String appliedPath) {
        if (appliedPath == null) {
            this.appliedPath = "@";
        } else {
            this.appliedPath = appliedPath;
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

    void setCVisualAttributes(String attrs) {
        this.cVisualAttributes = attrs;
    }

    String getCVisualAttributes() {
        return this.cVisualAttributes;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Date getDownloaded() {
        return downloaded;
    }

    public void setDownloaded(Date downloaded) {
        this.downloaded = downloaded;
    }

    void setFullName(String fullName) {
        this.fullName = fullName;
    }

    void setCPath(String cPath) {
        this.cPath = cPath;
    }
    
    void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    void setToolTip(String toolTip) {
        this.toolTip = toolTip;
    }

    void setHLevel(int level) {
        this.level = level;
    }

    int getHLevel() {
        return this.level;
    }

    void setSynonymCode(SynonymCode synonymCode) {
        if (synonymCode == null) {
            this.synonymCode = SynonymCode.NOT_SYNONYM;
        } else {
            this.synonymCode = synonymCode;
        }
    }

    SynonymCode getSynonymCode() {
        return this.synonymCode;
    }

    boolean isAlreadyLoaded() {
        return alreadyLoaded;
    }

    void setAlreadyLoaded(boolean alreadyLoaded) {
        this.alreadyLoaded = alreadyLoaded;
    }
    
    boolean isRejected() {
        return this.rejected;
    }

    void setRejected(boolean rejected) {
        this.rejected = rejected;
    }

    String[] getRejectionReasons() {
        if (this.rejectionReasons != null) {
            return rejectionReasons.toArray(
                    new String[this.rejectionReasons.size()]);
        } else {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
    }

    void addRejectionReason(String reason) {
        if (this.rejectionReasons == null) {
            this.rejectionReasons = new ArrayList<>();
        }
        this.rejectionReasons.add(reason);
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    void setUpdated(Date updated) {
        this.updated = updated;
    }

    Date getUpdated() {
        return this.updated;
    }
    
}
