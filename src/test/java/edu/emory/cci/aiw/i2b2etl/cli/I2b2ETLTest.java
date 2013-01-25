/*
 * #%L
 * AIW i2b2 ETL
 * %%
 * Copyright (C) 2012 Emory University
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
package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.ProtempaFactory;
import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import org.junit.*;
import org.protempa.CompoundLowLevelAbstractionDefinition;
import org.protempa.CompoundLowLevelAbstractionDefinition.ValueClassification;
import org.protempa.EventDefinition;
import org.protempa.FinderException;
import org.protempa.HighLevelAbstractionDefinition;
import org.protempa.LowLevelAbstractionDefinition;
import org.protempa.LowLevelAbstractionValueDefinition;
import org.protempa.Offsets;
import org.protempa.PrimitiveParameterDefinition;
import org.protempa.PropositionDefinition;
import org.protempa.Protempa;
import org.protempa.ProtempaStartupException;
import org.protempa.SimpleGapFunction;
import org.protempa.SlidingWindowWidthMode;
import org.protempa.TemporalExtendedParameterDefinition;
import org.protempa.TemporalExtendedPropositionDefinition;
import org.protempa.backend.BackendProviderSpecLoaderException;
import org.protempa.backend.ConfigurationsLoadException;
import org.protempa.backend.InvalidConfigurationException;
import org.protempa.proposition.interval.Relation;
import org.protempa.proposition.value.NominalValue;
import org.protempa.proposition.value.NumberValue;
import org.protempa.proposition.value.ValueComparator;
import org.protempa.proposition.value.ValueType;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuildException;
import org.protempa.query.handler.QueryResultsHandler;
import org.protempa.query.handler.test.DataProviderException;
import org.protempa.query.handler.test.DatabasePopulator;

/**
 * Integration tests for the i2b2 ETL. This assumes that there is an i2b2
 * instance somewhere to use for testing. Specify where it is in your
 * settings.xml file (future). 
 * 
 * @author Andrew Post
 */
public class I2b2ETLTest {

    /**
     * Executes the i2b2 ETL load.
     * 
     * @throws ProtempaStartupException if Protempa could not be initialized.
     * @throws IOException if there was a problem reading the Protempa
     * configuration file or the i2b2 query results handler configuration file.
     * @throws QueryBuildException if constructing the Protempa query failed.
     * @throws FinderException if executing the Protempa query failed.
     */
    @BeforeClass
    public static void setUp() throws ProtempaStartupException, IOException, 
            QueryBuildException, FinderException, DataProviderException, 
            SQLException, URISyntaxException, 
            BackendProviderSpecLoaderException, ConfigurationsLoadException, 
            InvalidConfigurationException {
        new DatabasePopulator().doPopulate();
        Protempa protempa = new ProtempaFactory().newInstance();
        try {
            File confXML = new I2b2ETLConfAsFile().getFile();
            DefaultQueryBuilder q = new DefaultQueryBuilder();
            
            EventDefinition ed = new EventDefinition("MyDiagnosis");
            ed.setDisplayName("My Diagnosis");
            ed.setInverseIsA("ICD9:719.52");
            
            PrimitiveParameterDefinition pd = 
                    new PrimitiveParameterDefinition("MyLabTest");
            pd.setDisplayName("My Lab Test");
            pd.setInverseIsA("LAB:8007694");
            pd.setValueType(ValueType.NUMBERVALUE);
            
            PrimitiveParameterDefinition pd2 =
                    new PrimitiveParameterDefinition("MyOtherLabTest");
            pd2.setDisplayName("My Other Lab Test");
            pd2.setInverseIsA("LAB:8400010");
            pd2.setValueType(ValueType.NUMBERVALUE);
            
            LowLevelAbstractionDefinition ld =
                    new LowLevelAbstractionDefinition("MyLabThreshold");
            ld.setDisplayName("My Lab Threshold");
            ld.addPrimitiveParameterId(pd.getId());
            LowLevelAbstractionValueDefinition vd =
                    new LowLevelAbstractionValueDefinition(ld, "MyLabThresholdHigh");
            vd.setValue(NominalValue.getInstance("High"));
            vd.setParameterComp("minThreshold", ValueComparator.GREATER_THAN);
            vd.setParameterValue("minThreshold", NumberValue.getInstance(600));
            ld.addValueDefinition(vd);
            LowLevelAbstractionValueDefinition vd3 =
                    new LowLevelAbstractionValueDefinition(ld, "MyLabThresholdOther");
            vd3.setValue(NominalValue.getInstance("Other"));
            vd3.setParameterComp("maxThreshold", ValueComparator.LESS_THAN_OR_EQUAL_TO);
            vd3.setParameterValue("maxThreshold", NumberValue.getInstance(600));
            ld.addValueDefinition(vd3);
            ld.setAlgorithmId("stateDetector");
            ld.setMaximumNumberOfValues(1);
            ld.setMinimumNumberOfValues(1);
            ld.setGapFunction(new SimpleGapFunction(0, null));
            ld.setSlidingWindowWidthMode(SlidingWindowWidthMode.RANGE);
            
            LowLevelAbstractionDefinition ld2 =
                    new LowLevelAbstractionDefinition("MyOtherLabThreshold");
            ld2.setDisplayName("My Other Lab Threshold");
            ld2.addPrimitiveParameterId("LAB:8400010");
            LowLevelAbstractionValueDefinition vd2 =
                    new LowLevelAbstractionValueDefinition(ld2, "MyOtherLabThresholdHigh");
            vd2.setValue(NominalValue.getInstance("High"));
            vd2.setParameterComp("minThreshold", ValueComparator.GREATER_THAN);
            vd2.setParameterValue("minThreshold", NumberValue.getInstance(400));
            LowLevelAbstractionValueDefinition vd4 =
                    new LowLevelAbstractionValueDefinition(ld2, "MyOtherLabThresholdOther");
            vd4.setValue(NominalValue.getInstance("Other"));
            vd4.setParameterComp("maxThreshold", ValueComparator.LESS_THAN_OR_EQUAL_TO);
            vd4.setParameterValue("maxThreshold", NumberValue.getInstance(400));
            ld2.setAlgorithmId("stateDetector");
            ld2.setGapFunction(new SimpleGapFunction(0, null));
            ld2.setSlidingWindowWidthMode(SlidingWindowWidthMode.DEFAULT);
            
            HighLevelAbstractionDefinition ldWrapper =
                    new HighLevelAbstractionDefinition("MyLabThresholdWrapper");
            ldWrapper.setDisplayName("My Lab Threshold");
            TemporalExtendedParameterDefinition td3 =
                    new TemporalExtendedParameterDefinition(ld.getId());
            td3.setValue(NominalValue.getInstance("High"));
            ldWrapper.add(td3);
            Relation rel2 = new Relation();
            ldWrapper.setRelation(td3, td3, rel2);
            
            CompoundLowLevelAbstractionDefinition clad =
                    new CompoundLowLevelAbstractionDefinition("Compound");
            clad.setDisplayName("Combined Thresholds");
            clad.setValueDefinitionMatchOperator(CompoundLowLevelAbstractionDefinition.ValueDefinitionMatchOperator.ANY);
            clad.addValueClassification(new ValueClassification("High", ld.getId(), "High"));
            clad.addValueClassification(new ValueClassification("High", ld2.getId(), "High"));
            clad.addValueClassification(new ValueClassification("Other", ld.getId(), "Other"));
            clad.addValueClassification(new ValueClassification("Other", ld2.getId(), "Other"));
            clad.setMinimumNumberOfValues(1);
            
            CompoundLowLevelAbstractionDefinition clad2 =
                    new CompoundLowLevelAbstractionDefinition("AtLeast2Compound");
            clad2.setDisplayName("At Least 2");
            clad2.addValueClassification(new ValueClassification("High", clad.getId(), "High"));
            clad2.addValueClassification(new ValueClassification("Other", clad.getId(), "Other"));
            clad2.setMinimumNumberOfValues(2);
            
            HighLevelAbstractionDefinition hd =
                    new HighLevelAbstractionDefinition("MyTemporalPattern");
            hd.setDisplayName("My Temporal Pattern");
            TemporalExtendedPropositionDefinition td1 =
                    new TemporalExtendedPropositionDefinition(ed.getId());
            TemporalExtendedPropositionDefinition td2 =
                    new TemporalExtendedPropositionDefinition(pd.getId());
            hd.add(td1);
            hd.add(td2);
            Relation rel = new Relation();
            hd.setRelation(td1, td2, rel);
            hd.setTemporalOffset(new Offsets());
            
            q.setPropositionDefinitions(
                    new PropositionDefinition[]{ed, pd, pd2, hd, ld, ldWrapper, ld2, clad, clad2, pd2, ld2});
            q.setPropositionIds(new String[]{ed.getId(), pd.getId(), hd.getId(), ldWrapper.getId(), ld2.getId(), clad.getId(), clad2.getId(), pd2.getId(), ld2.getId()});
            q.setId("i2b2 ETL Test Query");
            
            Query query = protempa.buildQuery(q);
            QueryResultsHandler tdqrh = new I2B2QueryResultsHandler(confXML);
            protempa.execute(query, tdqrh);
        } finally {
            protempa.close();
        }

    }

    @Test
    public void testSomeAspectOfI2b2Database() {
    }
    
    @AfterClass
    public static void shutdown() {
        //We leave the i2b2 load behind for post-mortum analyses.
    }
}
