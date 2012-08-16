package edu.emory.cci.aiw.i2b2etl;

import junit.framework.Assert;
import org.arp.javautil.arrays.Arrays;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.protempa.KnowledgeSource;
import org.protempa.PropositionDefinition;
import org.protempa.Protempa;

/**
 *
 * @author Andrew Post
 */
public class I2b2DerivedPropositionIdExtractorTest {

    private static Protempa protempa;

    @BeforeClass
    public static void setUp() throws Exception {
        protempa = new ProtempaFactory().newInstance();
    }

    @AfterClass
    public static void tearDown() {
        if (protempa != null) {
            protempa.close();
        }
    }

    @Test
    public void testEncounter() throws Exception {

        String[] expected = {
            "30DayReadmissionMult",
            "READMISSIONS:EncounterWithNoSubsequent30DayReadmissionOverlaps",
            "READMISSIONS:Encounter90DaysEarlier",
            "READMISSIONS:Encounter180DaysEarlier",
            "READMISSIONS:EncounterWithNoSubsequent30DayReadmissionBefore",
            "ADT:Visit",
            "READMISSIONS:EncounterWithSubsequent30DayReadmission",
            "READMISSIONS:FourthEncounterWithSubsequent30DayReadmission",
            "READMISSIONS:30DayReadmission",
            "EncounterPair",
            "READMISSIONS:SecondReadmit",
            "READMISSIONS:EncounterWithNoSubsequent30DayReadmission",
            "READMISSIONS:FrequentFlierEncounter"
        };
        String[] actual = computeActual("Encounter");

        Assert.assertEquals(Arrays.asSet(expected), Arrays.asSet(actual));
    }

    @Test
    public void testV58_1() throws Exception {
        String[] expected = {
            "READMISSIONS:Chemotherapy180DaysBeforeSurgery",
            "READMISSIONS:Chemotherapy365DaysBeforeSurgery"
        };
        String[] actual = computeActual("ICD9:V58.1");
        Assert.assertEquals(Arrays.asSet(expected), Arrays.asSet(actual));
    }

    private static String[] computeActual(String propId) throws Exception {
        KnowledgeSource ks = protempa.getKnowledgeSource();
        I2b2DerivedPropositionIdExtractor extractor =
                new I2b2DerivedPropositionIdExtractor(ks);
        PropositionDefinition encounterDef =
                ks.readPropositionDefinition(propId);
        PropositionDefinition[] defs =
                new PropositionDefinition[]{encounterDef};
        String[] actual = extractor.extractDerived(defs);
        return actual;
    }
}
