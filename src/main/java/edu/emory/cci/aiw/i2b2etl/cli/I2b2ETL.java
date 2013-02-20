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
