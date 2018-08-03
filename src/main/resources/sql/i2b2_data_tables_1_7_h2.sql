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
--------------------------------------------------------
--  File created - Wednesday-April-22-2015   
--------------------------------------------------------

--------------------------------------------------------
--  DDL for Table PROVIDER_DIMENSION
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "PROVIDER_DIMENSION" 
   (	"PROVIDER_ID" VARCHAR2(50), 
	"PROVIDER_PATH" VARCHAR2(700), 
	"NAME_CHAR" VARCHAR2(850), 
	"PROVIDER_BLOB" CLOB, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table REJECTED_OBSERVATION_FACT
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "REJECTED_OBSERVATION_FACT" 
   (	"ENCOUNTER_NUM" NUMBER(38,0), 
	"ENCOUNTER_ID" VARCHAR2(200), 
	"ENCOUNTER_ID_SOURCE" VARCHAR2(50), 
	"CONCEPT_CD" VARCHAR2(50), 
	"PATIENT_NUM" NUMBER(38,0), 
	"PATIENT_ID" VARCHAR2(200), 
	"PATIENT_ID_SOURCE" VARCHAR2(50), 
	"PROVIDER_ID" VARCHAR2(50), 
	"START_DATE" DATE, 
	"MODIFIER_CD" VARCHAR2(100), 
	"INSTANCE_NUM" NUMBER(18,0), 
	"VALTYPE_CD" VARCHAR2(50), 
	"TVAL_CHAR" VARCHAR2(255), 
	"NVAL_NUM" NUMBER(18,5), 
	"VALUEFLAG_CD" CHAR(50), 
	"QUANTITY_NUM" NUMBER(18,5), 
	"CONFIDENCE_NUM" NUMBER(18,0), 
	"OBSERVATION_BLOB" CLOB, 
	"UNITS_CD" VARCHAR2(50), 
	"END_DATE" DATE, 
	"LOCATION_CD" VARCHAR2(50), 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0), 
	"REASON" VARCHAR2(255)
   ) ;
--------------------------------------------------------
--  DDL for Table CONCEPT_DIMENSION
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "CONCEPT_DIMENSION" 
   (	"CONCEPT_PATH" VARCHAR2(700), 
	"CONCEPT_CD" VARCHAR2(50), 
	"NAME_CHAR" VARCHAR2(2000), 
	"CONCEPT_BLOB" CLOB, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table OBSERVATION_FACT
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "OBSERVATION_FACT" 
   (	"ENCOUNTER_NUM" NUMBER(38,0), 
	"PATIENT_NUM" NUMBER(38,0), 
	"CONCEPT_CD" VARCHAR2(50), 
	"PROVIDER_ID" VARCHAR2(50), 
	"START_DATE" DATE, 
	"MODIFIER_CD" VARCHAR2(100) DEFAULT '@', 
	"INSTANCE_NUM" NUMBER(18,0) DEFAULT '1', 
	"VALTYPE_CD" VARCHAR2(50), 
	"TVAL_CHAR" VARCHAR2(255), 
	"NVAL_NUM" NUMBER(18,5), 
	"VALUEFLAG_CD" VARCHAR2(50), 
	"QUANTITY_NUM" NUMBER(18,5), 
	"UNITS_CD" VARCHAR2(50), 
	"END_DATE" DATE, 
	"LOCATION_CD" VARCHAR2(50), 
	"OBSERVATION_BLOB" CLOB, 
	"CONFIDENCE_NUM" NUMBER(18,5), 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table PATIENT_MAPPING
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "PATIENT_MAPPING" 
   (	"PATIENT_IDE" VARCHAR2(200), 
	"PATIENT_IDE_SOURCE" VARCHAR2(50), 
	"PATIENT_NUM" NUMBER(38,0), 
	"PATIENT_IDE_STATUS" VARCHAR2(50), 
	"PROJECT_ID" VARCHAR2(50), 
	"UPLOAD_DATE" DATE, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;
--------------------------------------------------------
--  DDL for Table ARCHIVE_OBSERVATION_FACT
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "ARCHIVE_OBSERVATION_FACT" 
   (	"ENCOUNTER_NUM" NUMBER(38,0), 
	"PATIENT_NUM" NUMBER(38,0), 
	"CONCEPT_CD" VARCHAR2(50), 
	"PROVIDER_ID" VARCHAR2(50), 
	"START_DATE" DATE, 
	"MODIFIER_CD" VARCHAR2(100), 
	"INSTANCE_NUM" NUMBER(18,0), 
	"VALTYPE_CD" VARCHAR2(50), 
	"TVAL_CHAR" VARCHAR2(255), 
	"NVAL_NUM" NUMBER(18,5), 
	"VALUEFLAG_CD" VARCHAR2(50), 
	"QUANTITY_NUM" NUMBER(18,5), 
	"UNITS_CD" VARCHAR2(50), 
	"END_DATE" DATE, 
	"LOCATION_CD" VARCHAR2(50), 
	"OBSERVATION_BLOB" CLOB, 
	"CONFIDENCE_NUM" NUMBER(18,5), 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0), 
	"ARCHIVE_UPLOAD_ID" NUMBER(22,0)
   ) ;

--------------------------------------------------------
--  DDL for Table PATIENT_DIMENSION
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "PATIENT_DIMENSION" 
   (	"PATIENT_NUM" NUMBER(38,0), 
	"VITAL_STATUS_CD" VARCHAR2(50), 
	"BIRTH_DATE" DATE, 
	"DEATH_DATE" DATE, 
	"SEX_CD" VARCHAR2(50), 
	"AGE_IN_YEARS_NUM" NUMBER(38,0), 
	"LANGUAGE_CD" VARCHAR2(50), 
	"RACE_CD" VARCHAR2(50), 
	"MARITAL_STATUS_CD" VARCHAR2(50), 
	"RELIGION_CD" VARCHAR2(50), 
	"ZIP_CD" VARCHAR2(10), 
	"STATECITYZIP_PATH" VARCHAR2(700), 
	"INCOME_CD" VARCHAR2(50), 
	"PATIENT_BLOB" CLOB, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table MODIFIER_DIMENSION
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "MODIFIER_DIMENSION" 
   (	"MODIFIER_PATH" VARCHAR2(700), 
	"MODIFIER_CD" VARCHAR2(50), 
	"NAME_CHAR" VARCHAR2(2000), 
	"MODIFIER_BLOB" CLOB, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table ENCOUNTER_MAPPING
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "ENCOUNTER_MAPPING" 
   (	"ENCOUNTER_IDE" VARCHAR2(200), 
	"ENCOUNTER_IDE_SOURCE" VARCHAR2(50), 
	"PROJECT_ID" VARCHAR2(50), 
	"ENCOUNTER_NUM" NUMBER(38,0), 
	"PATIENT_IDE" VARCHAR2(200), 
	"PATIENT_IDE_SOURCE" VARCHAR2(50), 
	"ENCOUNTER_IDE_STATUS" VARCHAR2(50), 
	"UPLOAD_DATE" DATE, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;

--------------------------------------------------------
--  DDL for Table VISIT_DIMENSION
--------------------------------------------------------

  CREATE TABLE IF NOT EXISTS "VISIT_DIMENSION" 
   (	"ENCOUNTER_NUM" NUMBER(38,0), 
	"PATIENT_NUM" NUMBER(38,0), 
	"ACTIVE_STATUS_CD" VARCHAR2(50), 
	"START_DATE" DATE, 
	"END_DATE" DATE, 
	"INOUT_CD" VARCHAR2(50), 
	"LOCATION_CD" VARCHAR2(50), 
	"LOCATION_PATH" VARCHAR2(900), 
	"LENGTH_OF_STAY" NUMBER(38,0), 
	"VISIT_BLOB" CLOB, 
	"UPDATE_DATE" DATE, 
	"DOWNLOAD_DATE" DATE, 
	"IMPORT_DATE" DATE, 
	"SOURCESYSTEM_CD" VARCHAR2(50), 
	"UPLOAD_ID" NUMBER(38,0)
   ) ;
