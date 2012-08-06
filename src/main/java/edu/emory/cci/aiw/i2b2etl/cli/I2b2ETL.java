package edu.emory.cci.aiw.i2b2etl.cli;

import edu.emory.cci.aiw.i2b2etl.I2B2QueryResultsHandler;
import java.io.File;
import org.apache.commons.cli.CommandLine;
import org.protempa.*;
import org.protempa.cli.CLI;
import org.protempa.cli.CLIException;
import org.protempa.query.DefaultQueryBuilder;
import org.protempa.query.Query;
import org.protempa.query.QueryBuildException;
import org.protempa.query.handler.QueryResultsHandler;

/**
 *
 * @author Andrew Post
 */
public class I2b2ETL extends CLI {

    private static final String[] PROP_IDS = {
        "Patient",
        "PatientAll",
        "Encounter",
        "AttendingPhysician",
        "LaboratoryTest",
        "VitalSign",
        "ICD9:Procedures",
        "ICD9:Diagnoses",
        "MED:medications",
        "LAB:LabTest",
        "CPTCode"
    };

    public static void main(String[] args) {
        I2b2ETL etl = new I2b2ETL();
        etl.processOptionsAndArgs(args);
        etl.initializeExecuteAndClose();
    }

    public I2b2ETL() {
        super(new Argument[]{new Argument("i2b2Config", true)});
    }

    @Override
    public void execute(Protempa protempa, CommandLine commandLine)
            throws CLIException {
        File confXML = new File(commandLine.getArgs()[0]);

        try {
            DefaultQueryBuilder q = new DefaultQueryBuilder();
            q.setPropositionIds(PROP_IDS);
            Query query = protempa.buildQuery(q);
            QueryResultsHandler tdqrh = new I2B2QueryResultsHandler(confXML);
            protempa.execute(query, tdqrh);
        } catch (FinderException ex) {
            throw new CLIException("Error executing ETL job", ex);
        } catch (QueryBuildException ex) {
            throw new CLIException("Error building ETL job", ex);
        } finally {
            protempa.clear();
        }
    }
}
