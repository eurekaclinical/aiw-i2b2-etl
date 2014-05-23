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
package edu.emory.cci.aiw.i2b2etl.table;

import edu.emory.cci.aiw.i2b2etl.metadata.Concept;
import edu.emory.cci.aiw.i2b2etl.metadata.InvalidConceptCodeException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents records in the concept dimension.
 * 
 * The concept dimension has the following DDL:
 * <pre>
 * CREATE TABLE "CONCEPT_DIMENSION"
 * (
 * "CONCEPT_CD" VARCHAR2(50) NOT NULL ENABLE,
 * "CONCEPT_PATH" VARCHAR2(700) NOT NULL ENABLE,
 * "NAME_CHAR" VARCHAR2(2000),
 * "CONCEPT_BLOB" CLOB,
 * "UPDATE_DATE" DATE,
 * "DOWNLOAD_DATE" DATE,
 * "IMPORT_DATE" DATE,
 * "SOURCESYSTEM_CD" VARCHAR2(50),
 * "UPLOAD_ID" NUMBER(38,0),
 * CONSTRAINT "CONCEPT_DIMENSION_PK" PRIMARY KEY ("CONCEPT_PATH") ENABLE
 * )
 * </pre>
 * 
 * @author Andrew Post
 */
public class ConceptDimension {

    
    private static final Logger logger = Logger.getLogger(ConceptDimension.class.getName());

    public static void insertAll(Concept root, Connection cn) throws SQLException, InvalidConceptCodeException {

        int batchSize = 1000;
        int counter = 0;
        int commitSize = 10000;
        int commitCounter = 0;
        PreparedStatement ps = null;
        try {
            ps = cn.prepareStatement("insert into CONCEPT_DIMENSION(concept_cd,concept_path,name_char,concept_blob," +
                    "update_date,download_date,import_date,sourcesystem_cd,upload_id) values (?,?,?,?,?,?,?,?,?)");
            @SuppressWarnings("unchecked")
            Enumeration<Concept> emu = root.breadthFirstEnumeration();
            Timestamp importTimestamp = 
                    new Timestamp(System.currentTimeMillis());
            while (emu.hasMoreElements()) {

                Concept concept = emu.nextElement();
                if (concept.isInUse()) {
                    ArrayList<String> paths = concept.getHierarchyPaths();
                    if (paths != null) {
                        for (int i = 0; i < paths.size(); i++) {
                            ps.setString(1, concept.getConceptCode());
                            //ps.setString(2, concept.getI2B2Path());
                            ps.setString(2, paths.get(i));
                            ps.setString(3, concept.getDisplayName());
                            ps.setObject(4, null);
                            ps.setTimestamp(5, null);
                            ps.setTimestamp(6, null);
                            ps.setTimestamp(7, importTimestamp);
                            ps.setString(8, concept.getSourceSystemCode());
                            ps.setObject(9, null);
                            logger.log(Level.FINEST, "DB_CD_INSERT {0}", concept);
                            counter++;
                            commitCounter++;
                            ps.addBatch();
                            ps.clearParameters();
                            if (counter >= batchSize) {
                                ps.executeBatch();
                                ps.clearBatch();
                                counter = 0;
                            }
                            if (commitCounter >= commitSize) {
                                cn.commit();
                                commitCounter = 0;
                            }
                        }
                    } else {
                        ps.setString(1, concept.getConceptCode());
                        ps.setString(2, concept.getI2B2Path());
                        //ps.setString(2, paths.get(i));
                        ps.setString(3, concept.getDisplayName());
                        ps.setObject(4, null);
                        ps.setTimestamp(5, null);
                        ps.setTimestamp(6, null);
                        ps.setTimestamp(7, importTimestamp);
                        ps.setString(8, concept.getSourceSystemCode());
                        ps.setObject(9, null);
                        logger.log(Level.FINEST, "DB_CD_INSERT {0}", concept);
                        counter++;
                        commitCounter++;
                        ps.addBatch();
                        ps.clearParameters();
                        if (counter >= batchSize) {
                            ps.executeBatch();
                            ps.clearBatch();
                            counter = 0;
                        }
                        if (commitCounter >= commitSize) {
                            cn.commit();
                            commitCounter = 0;
                        }
                    }
                }
            }
            if (counter > 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
            if (commitCounter > 0) {
                cn.commit();
            }
            ps.close();
            ps = null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }
}
