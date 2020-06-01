package edu.emory.cci.aiw.i2b2etl.dest.table;

/*-
 * #%L
 * AIW OMOP ETL
 * %%
 * Copyright (C) 2019 Emory University
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

import java.util.logging.Logger;

/**
 *
 * @author Nita
 */
public class PhenotypeObsFactTableHandler {

	private static final Logger LOGGER = Logger.getLogger(PhenotypeObsFactTableHandler.class.getName());
    private String insertStatement;
    
    private int numCols;
	
	public PhenotypeObsFactTableHandler() {
		LOGGER.info("Constructing ObsFactTableHandler");
	}

	public String getInsertStatement(String tableName) {
		switch(tableName) {
		case "PHENOTYPE_OBSERVATION_FACT":
			insertStatement =  "insert into phenotypedata.phenotype_observation_fact "
					+ "(encounter_num, patient_num, concept_cd, provider_id, start_date, "
					+ "modifier_cd, instance_num, valtype_cd, tval_char, nval_num, "
					+ "valueflag_cd, quantity_num, units_cd, end_date, location_cd, "
					+ "observation_blob, confidence_num, update_date, download_date, import_date, "
					+ "sourcesystem_cd, upload_id)"
	                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			break;
		}
		return insertStatement;
	}
	
	public void setInsertStatement(String insertStatement) {
		this.insertStatement = insertStatement;
	}

	public int getNumCols(String tableName) {
		switch(tableName) {
		case "PHENOTYPE_OBSERVATION_FACT":
			numCols = 22;
			break;
		}
		return numCols;
	}	    
}
