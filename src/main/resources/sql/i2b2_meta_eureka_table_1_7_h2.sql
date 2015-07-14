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
--  DDL for Table TABLE_ACCESS
--------------------------------------------------------

  CREATE TABLE TABLE_ACCESS 
   (	C_TABLE_CD VARCHAR2(50), 
	C_TABLE_NAME VARCHAR2(50), 
	C_PROTECTED_ACCESS CHAR(1), 
	C_HLEVEL NUMBER(22,0), 
	C_FULLNAME VARCHAR2(700), 
	C_NAME VARCHAR2(2000), 
	C_SYNONYM_CD CHAR(1), 
	C_VISUALATTRIBUTES CHAR(3), 
	C_TOTALNUM NUMBER(22,0), 
	C_BASECODE VARCHAR2(50), 
	C_METADATAXML CLOB, 
	C_FACTTABLECOLUMN VARCHAR2(50), 
	C_DIMTABLENAME VARCHAR2(50), 
	C_COLUMNNAME VARCHAR2(50), 
	C_COLUMNDATATYPE VARCHAR2(50), 
	C_OPERATOR VARCHAR2(10), 
	C_DIMCODE VARCHAR2(700), 
	C_COMMENT CLOB, 
	C_TOOLTIP VARCHAR2(900), 
	C_ENTRY_DATE DATE, 
	C_CHANGE_DATE DATE, 
	C_STATUS_CD CHAR(1), 
	VALUETYPE_CD VARCHAR2(50)
   );

--------------------------------------------------------
--  Constraints for Table TABLE_ACCESS
--------------------------------------------------------

  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_DIMCODE SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_OPERATOR SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_COLUMNDATATYPE SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_COLUMNNAME SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_DIMTABLENAME SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_FACTTABLECOLUMN SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_VISUALATTRIBUTES SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_SYNONYM_CD SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_NAME SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_FULLNAME SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_HLEVEL SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_TABLE_NAME SET NOT NULL;
  ALTER TABLE TABLE_ACCESS ALTER COLUMN C_TABLE_CD SET NOT NULL;