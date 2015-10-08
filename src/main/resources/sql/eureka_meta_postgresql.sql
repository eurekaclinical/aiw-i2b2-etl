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

CREATE SCHEMA EUREKA;

CREATE OR REPLACE FUNCTION EUREKA.EK_CLEAR_C_TOTALNUM(tableName text) RETURNS VOID AS $body$
    BEGIN
        EXECUTE 'UPDATE ' || tableName || ' SET C_TOTALNUM=NULL';
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;
    END;
$body$
LANGUAGE PLPGSQL
;
    
CREATE OR REPLACE FUNCTION EK_UPDATE_C_TOTALNUM(tableName text) RETURNS VOID AS $body$
    BEGIN
        EXECUTE 'UPDATE ' || tableName || ' a3
        SET a3.c_totalnum =
          (SELECT COUNT(DISTINCT patient_num)
          FROM i2b217data.observation_fact a1
          JOIN i2b217data.concept_dimension a2
          ON (a1.concept_cd = a2.concept_cd)
          WHERE a2.concept_path LIKE a3.c_dimcode || ''%'')
          WHERE UPPER(a3.c_operator)=''LIKE''
          AND UPPER(a3.c_facttablecolumn)=''CONCEPT_CD''
          AND UPPER(a3.c_tablename)=''CONCEPT_DIMENSION''
          AND UPPER(a3.c_columnname)=''CONCEPT_PATH''
          AND a3.c_columndatatype=''T''';
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;
    END;
$body$
LANGUAGE PLPGSQL
;
