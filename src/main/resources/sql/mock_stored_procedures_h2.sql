---
-- #%L
-- AIW i2b2 ETL
-- %%
-- Copyright (C) 2012 - 2015 Emory University
-- %%
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- #L%
---
CREATE SCHEMA IF NOT EXISTS EUREKA;

DROP ALIAS IF EXISTS EUREKA.EK_INSERT_PID_MAP_FROMTEMP;
CREATE ALIAS EUREKA.EK_INSERT_PID_MAP_FROMTEMP AS $$
void insertPIDMapFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INSERT_EID_MAP_FROMTEMP;
CREATE ALIAS EUREKA.EK_INSERT_EID_MAP_FROMTEMP AS $$
void insertEIDMapFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INS_PATIENT_FROMTEMP;
CREATE ALIAS EUREKA.EK_INS_PATIENT_FROMTEMP AS $$
void insertPatientFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INS_ENC_VISIT_FROMTEMP;
CREATE ALIAS EUREKA.EK_INS_ENC_VISIT_FROMTEMP AS $$
void insertEncVisitFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INS_PROVIDER_FROMTEMP;
CREATE ALIAS EUREKA.EK_INS_PROVIDER_FROMTEMP AS $$
void insertProviderFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INS_CONCEPT_FROMTEMP;
CREATE ALIAS EUREKA.EK_INS_CONCEPT_FROMTEMP AS $$
void insertConceptFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_INS_MODIFIER_FROMTEMP;
CREATE ALIAS EUREKA.EK_INS_MODIFIER_FROMTEMP AS $$
void insertModifierFromTemp(Connection conn, String tableName, int uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_UPDATE_OBSERVATION_FACT;
CREATE ALIAS EUREKA.EK_UPDATE_OBSERVATION_FACT AS $$
void updateObservationFact(Connection conn, String tableName, String tableName2, long appendFlag, long uploadId) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_PRE_HOOK;
CREATE ALIAS EUREKA.EK_PRE_HOOK AS $$
void preHook(Connection conn) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_POST_HOOK;
CREATE ALIAS EUREKA.EK_POST_HOOK AS $$
void postHook(Connection conn) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_DISABLE_INDEXES;
CREATE ALIAS EUREKA.EK_DISABLE_INDEXES AS $$
void disableIndexes(Connection conn) {
}
$$;

DROP ALIAS IF EXISTS EUREKA.EK_ENABLE_INDEXES;
CREATE ALIAS EUREKA.EK_ENABLE_INDEXES AS $$
void enableIndexes(Connection conn) {
}
$$;
