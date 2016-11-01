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
package edu.emory.cci.aiw.i2b2etl.dest;

import static edu.emory.cci.aiw.i2b2etl.dest.AbstractI2b2DestTest.getProtempaFactory;
import java.io.File;
import java.io.FileOutputStream;

import org.junit.BeforeClass;

import org.protempa.CompoundLowLevelAbstractionDefinition;
import org.protempa.EventDefinition;
import org.protempa.HighLevelAbstractionDefinition;
import org.protempa.LowLevelAbstractionDefinition;
import org.protempa.LowLevelAbstractionValueDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.ProtempaException;
import org.protempa.SimpleGapFunction;
import org.protempa.SliceDefinition;
import org.protempa.SlidingWindowWidthMode;
import org.protempa.TemporalExtendedParameterDefinition;
import org.protempa.TemporalExtendedPropositionDefinition;
import org.protempa.TemporalPatternOffset;
import org.protempa.ValueClassification;
import org.protempa.proposition.interval.Relation;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.NumberValue;
import org.protempa.proposition.value.ValueComparator;
import org.protempa.query.DefaultQueryBuilder;

/**
 * Data validation tests for the i2b2 ETL. The test initiates Protempa to access
 * the test data and execute a query before AIW ETL loads the processed data
 * into an H2 database. The new loaded data is compared to the one expected
 * using DbUnit.
 *
 * @author Andrew Post
 */
public class I2b2LoadTest extends AbstractI2b2DestLoadTest {

    /**
     * Executes the i2b2 ETL load.
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        DefaultQueryBuilder q = new DefaultQueryBuilder();

        EventDefinition ed = new EventDefinition("MyDiagnosis");
        ed.setDisplayName("My Diagnosis");
        ed.setInverseIsA("ICD9:719.52");

        LowLevelAbstractionDefinition ld
                = new LowLevelAbstractionDefinition("MyLabThreshold");
        ld.setDisplayName("My Lab Threshold");
        ld.addPrimitiveParameterId("LAB:8007694");
        LowLevelAbstractionValueDefinition vd
                = new LowLevelAbstractionValueDefinition(ld, "MyLabThresholdHigh");
        vd.setValue(NominalValue.getInstance("High"));
        vd.setParameterComp("minThreshold", ValueComparator.GREATER_THAN);
        vd.setParameterValue("minThreshold", NumberValue.getInstance(600));
        ld.addValueDefinition(vd);
        LowLevelAbstractionValueDefinition vd3
                = new LowLevelAbstractionValueDefinition(ld, "MyLabThresholdOther");
        vd3.setValue(NominalValue.getInstance("Other"));
        vd3.setParameterComp("maxThreshold", ValueComparator.LESS_THAN_OR_EQUAL_TO);
        vd3.setParameterValue("maxThreshold", NumberValue.getInstance(600));
        ld.addValueDefinition(vd3);
        ld.setAlgorithmId("stateDetector");
        ld.setMaximumNumberOfValues(1);
        ld.setMinimumNumberOfValues(1);
        ld.setGapFunction(new SimpleGapFunction(0, null));
        ld.setSlidingWindowWidthMode(SlidingWindowWidthMode.RANGE);

        LowLevelAbstractionDefinition ld2
                = new LowLevelAbstractionDefinition("MyOtherLabThreshold");
        ld2.setDisplayName("My Other Lab Threshold");
        ld2.addPrimitiveParameterId("LAB:8400010");
        LowLevelAbstractionValueDefinition vd2
                = new LowLevelAbstractionValueDefinition(ld2, "MyOtherLabThresholdHigh");
        vd2.setValue(NominalValue.getInstance("High"));
        vd2.setParameterComp("minThreshold", ValueComparator.GREATER_THAN);
        vd2.setParameterValue("minThreshold", NumberValue.getInstance(300));
        LowLevelAbstractionValueDefinition vd4
                = new LowLevelAbstractionValueDefinition(ld2, "MyOtherLabThresholdOther");
        vd4.setValue(NominalValue.getInstance("Other"));
        vd4.setParameterComp("maxThreshold", ValueComparator.LESS_THAN_OR_EQUAL_TO);
        vd4.setParameterValue("maxThreshold", NumberValue.getInstance(300));
        ld2.setAlgorithmId("stateDetector");
        ld2.setGapFunction(new SimpleGapFunction(0, null));
        ld2.setSlidingWindowWidthMode(SlidingWindowWidthMode.DEFAULT);

        HighLevelAbstractionDefinition ldWrapper
                = new HighLevelAbstractionDefinition("MyLabThresholdWrapper");
        ldWrapper.setDisplayName("My Lab Threshold");
        TemporalExtendedParameterDefinition td3
                = new TemporalExtendedParameterDefinition(ld.getId());
        td3.setValue(NominalValue.getInstance("High"));
        ldWrapper.add(td3);
        Relation rel2 = new Relation();
        ldWrapper.setRelation(td3, td3, rel2);

        CompoundLowLevelAbstractionDefinition clad
                = new CompoundLowLevelAbstractionDefinition("Compound");
        clad.setDisplayName("Combined Thresholds");
        clad.setValueDefinitionMatchOperator(CompoundLowLevelAbstractionDefinition.ValueDefinitionMatchOperator.ANY);
        clad.addValueClassification(new ValueClassification("High", ld.getId(), "High"));
        clad.addValueClassification(new ValueClassification("High", ld2.getId(), "High"));
        clad.addValueClassification(new ValueClassification("Other", ld.getId(), "Other"));
        clad.addValueClassification(new ValueClassification("Other", ld2.getId(), "Other"));
        clad.setGapFunction(new SimpleGapFunction(Integer.valueOf(0), null));
        clad.setConcatenable(false);
        clad.setMinimumNumberOfValues(1);

        CompoundLowLevelAbstractionDefinition clad2
                = new CompoundLowLevelAbstractionDefinition("AtLeast2Compound");
        clad2.setDisplayName("At Least 2 consecutive");
        clad2.addValueClassification(new ValueClassification("High", clad.getId(), "High"));
        clad2.addValueClassification(new ValueClassification("Other", clad.getId(), "Other"));
        clad2.setGapFunction(new SimpleGapFunction(Integer.valueOf(0), null));
        //clad2.setConcatenable(false);
        clad2.setMinimumNumberOfValues(2);

        SliceDefinition sd = new SliceDefinition("FirstAndOrSecondSlice");
        sd.setDisplayName("First and/or second");
        TemporalExtendedParameterDefinition tepd
                = new TemporalExtendedParameterDefinition(clad.getId(),
                        NominalValue.getInstance("High"));
        sd.add(tepd);
        sd.setMinIndex(0);
        sd.setMaxIndex(2);

        HighLevelAbstractionDefinition hd
                = new HighLevelAbstractionDefinition("MyTemporalPattern");
        hd.setDisplayName("My Temporal Pattern");
        TemporalExtendedPropositionDefinition td1
                = new TemporalExtendedPropositionDefinition(ed.getId());
        TemporalExtendedPropositionDefinition td2
                = new TemporalExtendedPropositionDefinition("LAB:8007694");
        hd.add(td1);
        hd.add(td2);
        Relation rel = new Relation();
        hd.setRelation(td1, td2, rel);
        hd.setTemporalOffset(new TemporalPatternOffset());

        q.setPropositionDefinitions(
                new PropositionDefinition[]{ed, hd, ld, ldWrapper, ld2, clad, clad2, ld2, sd});
        q.setPropositionIds(new String[]{ed.getId(), hd.getId(), ldWrapper.getId(), ld2.getId(), clad.getId(), clad2.getId(), ld2.getId(), sd.getId(), "ICD9:Diagnoses", "ICD9:Procedures", "LAB:LabTest", "Encounter", "MED:medications", "VitalSign", "PatientDetails", "Provider"});
        q.setName("i2b2 ETL Test Query");

        try {
            getProtempaFactory().execute(q);
        } catch (ProtempaException ex) {
            File file = File.createTempFile("i2b2LoadTest", ".xml");
            try (FileOutputStream out = new FileOutputStream(file)) {
                getProtempaFactory().exportI2b2DataSchema(out);
                System.err.println("Dumped i2b2 data schema to " + file.getAbsolutePath());
            }
            throw ex;
        }

        setExpectedDataSet("/truth/i2b2LoadTestData.xml");
    }

}
