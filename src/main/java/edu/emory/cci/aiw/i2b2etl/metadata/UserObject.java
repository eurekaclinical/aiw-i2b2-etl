package edu.emory.cci.aiw.i2b2etl.metadata;

import edu.emory.cci.aiw.i2b2etl.util.CodeUtil;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.protempa.proposition.value.Value;

public class UserObject {

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

    public boolean isInDataSource() {
        return inDataSource;
    }

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

    public ConceptOperator getOperator() {
        if (isInDataSource() || isDerived()) {
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
    
    String getDimCode() {
        return this.dimCode;
    }
    
    boolean isCopy() {
        return this.copy;
    }
}
