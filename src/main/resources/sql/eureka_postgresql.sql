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

CREATE OR REPLACE FUNCTION EUREKA.EK_INS_PROVIDER_FROMTEMP ( tempProviderTableName text, upload_id bigint)  RETURNS VOID AS $body$
    BEGIN
        EXECUTE '
            UPDATE provider_dimension SET provider_id = temp.provider_id,
                                name_char = temp.name_char,
                                provider_blob = temp.provider_blob,
                                update_date = temp.update_date,
                                download_date = temp.download_date,
                                import_date = now(),
                                sourcesystem_cd = temp.sourcesystem_cd,
                                upload_id = ' || upload_id || ' 
            FROM ' || tempProviderTableName || ' temp 
            WHERE temp.provider_path = provider_dimension.provider_path 
                AND coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) >= coalesce(provider_dimension.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY''))';
        EXECUTE '
             INSERT INTO provider_dimension (
                                provider_id,
                                provider_path,
                                name_char,
                                provider_blob,
                                update_date,
                                download_date,
                                import_date,
                                sourcesystem_cd,
                                upload_id) 
            SELECT 
                    temp2.provider_id,
                    temp2.provider_path,
                    temp2.name_char,
                    temp2.provider_blob,
                    temp2.update_date,
                    temp2.download_date,
                    now(),
                    temp2.sourcesystem_cd, 
                    '|| upload_id ||' 
            FROM ' || tempProviderTableName || ' temp2 
            WHERE temp2.provider_path NOT IN (SELECT provider_dimension.provider_path from provider_dimension);
                 ';
        ANALYZE provider_dimension;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;       
    END;
$body$
LANGUAGE PLPGSQL
;


CREATE OR REPLACE FUNCTION EUREKA.EK_INS_CONCEPT_FROMTEMP ( tempConceptTableName text, upload_id bigint)  RETURNS VOID AS $body$
BEGIN 
        EXECUTE '
            UPDATE concept_dimension SET concept_cd = temp.concept_cd,
                    name_char = temp.name_char,
                    concept_blob = temp.concept_blob,
                    update_date = temp.update_date,
                    download_date = temp.DOWNLOAD_DATE,
                    import_date = now(),
                    sourcesystem_cd = temp.SOURCESYSTEM_CD,
                    upload_id = ' || upload_id || '
            FROM ' || tempConceptTableName || ' temp 
            WHERE coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) >= coalesce(concept_dimension.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY''))
                AND temp.concept_path = concept_dimension.concept_path';
        EXECUTE '
            INSERT INTO concept_dimension (                  
                concept_cd,
                concept_path,
                name_char,
                concept_blob,
                update_date,
                download_date,
                import_date,
                sourcesystem_cd,
                upload_id)
            SELECT temp2.concept_cd,
                temp2.concept_path,
                temp2.name_char,
                temp2.concept_blob,
                temp2.update_date,
                temp2.download_date,
                now(),
                temp2.sourcesystem_cd, 
                '|| UPLOAD_ID ||' 
            FROM ' || tempConceptTableName || ' temp2 
            WHERE temp2.concept_path NOT IN (SELECT concept_dimension.concept_path from concept_dimension);
            ';
        ANALYZE concept_dimension;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;       
    END;
$body$
LANGUAGE PLPGSQL
;

CREATE OR REPLACE FUNCTION EUREKA.EK_INS_MODIFIER_FROMTEMP ( tempModifierTableName text, upload_id bigint)  RETURNS VOID AS $body$
BEGIN 
        EXECUTE '
            UPDATE modifier_dimension SET modifier_cd = temp.modifier_cd,
                    name_char = temp.name_char,
                    modifier_blob = temp.modifier_blob,
                    update_date = temp.update_date,
                    download_date = temp.download_date,
                    import_date = now(),
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || '
            FROM ' || tempModifierTableName || ' temp 
            WHERE temp.update_date >= modifier_dimension.update_date 
                AND temp.modifier_path = modifier_dimension.modifier_path';
        EXECUTE '
            INSERT INTO modifier_dimension (
                modifier_cd,
                modifier_path,
                name_char,
                modifier_blob,
                update_date,
                download_date,
                import_date,
                sourcesystem_cd,
                upload_id)
            SELECT temp2.modifier_cd,
                temp2.modifier_path,
                temp2.name_char,
                temp2.modifier_blob,
                temp2.update_date,
                temp2.download_date,
                now(),
                temp2.sourcesystem_cd, 
                '|| upload_id ||' 
            FROM ' || tempModifierTableName || ' temp2 
            WHERE temp2.modifier_path NOT IN (SELECT modifier_dimension.modifier_path from modifier_dimension)
        ';
        ANALYZE modifier_dimension;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;       
    END;
$body$
LANGUAGE PLPGSQL
;


CREATE OR REPLACE FUNCTION EUREKA.EK_INS_ENC_VISIT_FROMTEMP ( tempTableName text, upload_id bigint)  RETURNS VOID AS $body$
BEGIN
        LOCK TABLE encounter_mapping IN EXCLUSIVE MODE NOWAIT ;
        --Create new encounter(encounter_mapping) if temp table encounter_ide does not exists
        -- in encounter_mapping table. -- jk added project id
        EXECUTE ' 
            insert into encounter_mapping (
                encounter_ide,
                encounter_ide_source,
                encounter_num,
                patient_ide,
                patient_ide_source,
                encounter_ide_status,
                project_id,
                upload_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            select temp.encounter_id, 
                temp.encounter_id_source, 
                cast(temp.encounter_id as bigint),
                temp.patient_id,
                temp.patient_id_source,
                ''A'',
                ''@'' project_id, 
                ' || upload_id || '                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            from ' || tempTableName || ' temp                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            where not exists (
                SELECT em.encounter_ide 
                from encounter_mapping em 
                where em.encounter_ide = temp.encounter_id 
                    and em.encounter_ide_source = temp.encounter_id_source
                    and em.patient_ide = temp.patient_id
                    and em.patient_ide_source = temp.patient_id_source)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
            and encounter_id_source = ''HIVE'' ';
            
        -- update encounter_num for temp table
        EXECUTE '
            UPDATE ' || tempTableName || ' tempTableName SET encounter_num = em.encounter_num 
            FROM encounter_mapping em WHERE 
                em.encounter_ide = tempTableName.encounter_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
                and em.encounter_ide_source = tempTableName.encounter_id_source                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                and coalesce(em.patient_ide_source,'''') = coalesce(tempTableName.patient_id_source,'''')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                and coalesce(em.patient_ide,'''') = coalesce(tempTableName.patient_id,'''') ';
                
        EXECUTE '
            UPDATE visit_dimension SET start_date = temp.start_date,
                end_date = temp.end_date,
                inout_cd = temp.inout_cd,
                location_cd = temp.location_cd,
                visit_blob = temp.visit_blob,
                update_date = temp.update_date,
                download_date = temp.download_date,
                import_date = now(),
                sourcesystem_cd = temp.sourcesystem_cd,
                upload_id = ' || upload_id || ',
                length_of_stay = temp.length_of_stay
            FROM ' || tempTableName || ' temp 
            WHERE coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) >= coalesce(visit_dimension.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY''))
                AND temp.encounter_num = visit_dimension.encounter_num'; 
        
        -- jk: added project_id='@' to WHERE clause... need to support projects...
        EXECUTE '
            insert into visit_dimension (
                encounter_num,
                patient_num,
                START_DATE,
                END_DATE,
                INOUT_CD,
                LOCATION_CD,
                VISIT_BLOB,
                UPDATE_DATE,
                DOWNLOAD_DATE,
                IMPORT_DATE,
                SOURCESYSTEM_CD,
                UPLOAD_ID,
                LENGTH_OF_STAY)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
            select temp.encounter_num, 
                pm.patient_num,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                temp.START_DATE,
                temp.END_DATE,
                temp.INOUT_CD,
                temp.LOCATION_CD,
                temp.VISIT_BLOB,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                temp.update_date,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                temp.download_date,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                now(),                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                temp.sourcesystem_cd, '
                || upload_id || ',
                temp.LENGTH_OF_STAY
            from ' || tempTableName || '  temp, patient_mapping pm                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
            where temp.encounter_num IS NOT NULL
            and temp.encounter_num not in (
                SELECT encounter_num 
                from visit_dimension vd
            ) 
            and pm.patient_ide = temp.patient_id 
            and pm.patient_ide_source = temp.patient_id_source';
        ANALYZE visit_dimension;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM; 
    END;
$body$
LANGUAGE PLPGSQL
;


CREATE OR REPLACE FUNCTION EUREKA.EK_INS_PATIENT_FROMTEMP ( tempTableName text, upload_id bigint)  RETURNS VOID AS $body$
DECLARE
        maxPatientNum bigint; 
BEGIN 
        LOCK TABLE patient_mapping IN EXCLUSIVE MODE NOWAIT;
        -- Create new patient(patient_mapping) if temp table patient_ide does not exists 
        -- in patient_mapping table.
        EXECUTE '
            insert into patient_mapping (
                patient_ide,
                patient_ide_source,
                patient_num,
                patient_ide_status,
                upload_id)
            select temp.patient_id, 
                temp.patient_id_source, 
                cast(temp.patient_id as bigint), 
                ''A'',
                '|| upload_id ||'
            from ' || tempTableName || ' temp 
            WHERE NOT EXISTS (
                SELECT patient_ide 
                from patient_mapping pm 
                where pm.patient_ide = temp.patient_id 
                    and pm.patient_ide_source = temp.patient_id_source)
            and temp.patient_id_source = ''HIVE'' ';
        
        -- update patient_num for temp table
        EXECUTE '
            UPDATE ' || tempTableName || ' SET patient_num = patient_mapping.patient_num FROM patient_mapping 
                                    WHERE patient_mapping.patient_ide = '|| tempTableName ||'.patient_id
                                    AND patient_mapping.patient_ide_source = '|| tempTableName ||'.patient_id_source';

        EXECUTE '
            UPDATE patient_dimension SET vital_status_cd = temp.vital_status_cd, 
                    birth_date = temp.birth_date, 
                    death_date = temp.death_date,
                    sex_cd = temp.sex_cd,
                    age_in_years_num = temp.age_in_years_num,
                    language_cd = temp.language_cd,
                    race_cd = temp.race_cd,
                    marital_status_cd = temp.marital_status_cd,
                    religion_cd = temp.religion_cd,
                    zip_cd = temp.zip_cd,
                    statecityzip_path = temp.statecityzip_path,
                    patient_blob = temp.patient_blob,
                    update_date = temp.update_date,
                    download_date = temp.download_date,
                    import_date = now(),
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || ' 
            FROM ' || tempTableName || ' temp 
            WHERE coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) >= coalesce(patient_dimension.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY''))
                AND temp.patient_num = patient_dimension.patient_num';
        EXECUTE '
            INSERT INTO patient_dimension (
                    PATIENT_NUM,
                    VITAL_STATUS_CD,
                    BIRTH_DATE,
                    DEATH_DATE,
                    SEX_CD, 
                    AGE_IN_YEARS_NUM,
                    LANGUAGE_CD,
                    RACE_CD,
                    MARITAL_STATUS_CD, 
                    RELIGION_CD,
                    ZIP_CD,
                    STATECITYZIP_PATH,
                    PATIENT_BLOB,
                    UPDATE_DATE,
                    DOWNLOAD_DATE,
                    IMPORT_DATE,
                    SOURCESYSTEM_CD,
                    UPLOAD_ID)
                SELECT temp2.patient_num,
                    temp2.VITAL_STATUS_CD, 
                    temp2.BIRTH_DATE, 
                    temp2.DEATH_DATE,
                    temp2.SEX_CD, 
                    temp2.AGE_IN_YEARS_NUM,
                    temp2.LANGUAGE_CD,
                    temp2.RACE_CD,
                    temp2.MARITAL_STATUS_CD, 
                    temp2.RELIGION_CD,
                    temp2.ZIP_CD,
                    temp2.STATECITYZIP_PATH,
                    temp2.PATIENT_BLOB,
                    temp2.update_date,
                    temp2.download_date,
                    now(), 
                    temp2.sourcesystem_cd,
                    '|| UPLOAD_ID || '
                FROM ' || tempTableName || ' temp2 
                WHERE temp2.patient_num NOT IN (SELECT patient_dimension.patient_num from patient_dimension) 
                    AND temp2.patient_num IS NOT NULL
                    AND temp2.patient_num::text <> ''''';
        ANALYZE patient_dimension;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;       
    END;
$body$
LANGUAGE PLPGSQL
;




CREATE OR REPLACE FUNCTION EUREKA.EK_UPDATE_OBSERVATION_FACT ( upload_temptable_name text, upload_temptable_name_c text, upload_id bigint, appendFlag bigint)  RETURNS VOID AS $body$
BEGIN
        EXECUTE 'INSERT INTO ' || upload_temptable_name_c || '
          (SELECT em.encounter_num,
                  temp.encounter_id,
                  temp.encounter_id_source,
                  temp.concept_cd,
                  pm.patient_num,
                  temp.patient_id,
                  temp.patient_id_source,
                  temp.provider_id,
                  temp.start_date,
                  temp.modifier_cd,
                  temp.instance_num,
                  temp.valtype_cd,
                  temp.tval_char,
                  temp.nval_num,
                  temp.valueflag_cd,
                  temp.quantity_num,
                  temp.confidence_num,
                  temp.observation_blob,
                  temp.units_cd,
                  temp.end_date,
                  temp.location_cd,
                  temp.update_date,
                  temp.download_date,
                  temp.import_date,
                  temp.sourcesystem_cd,
                  temp.upload_id
                FROM ' || upload_temptable_name || ' temp
                LEFT OUTER JOIN encounter_mapping em
                ON (em.encounter_ide = temp.encounter_id
                            and em.encounter_ide_source = temp.encounter_id_source
                            and em.project_id=''@'')
                LEFT OUTER JOIN patient_mapping pm
                ON (pm.patient_ide = temp.patient_id
                            and pm.patient_ide_source = temp.patient_id_source
                            and pm.project_id=''@''))';

        EXECUTE 'TRUNCATE TABLE ' || upload_temptable_name;
        
        -- if the append is true, then do the update else do insert all
        IF ( appendFlag = 0 ) THEN
            --Transfer all rows from temp_obsfact to observation_fact
            EXECUTE
                'INSERT INTO observation_fact(
                    encounter_num,
                    concept_cd, 
                    patient_num,
                    provider_id, 
                    start_date,
                    modifier_cd,
                    instance_num,
                    valtype_cd,
                    tval_char,
                    nval_num,
                    valueflag_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                    quantity_num,
                    confidence_num,
                    observation_blob,
                    units_cd,
                    end_date,
                    location_cd, 
                    update_date,
                    download_date,
                    import_date,
                    sourcesystem_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                    upload_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                SELECT encounter_num,
                    concept_cd, 
                    patient_num,
                    provider_id, 
                    start_date,
                    modifier_cd,
                    instance_num,
                    valtype_cd,
                    tval_char,
                    nval_num,
                    valueflag_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                    quantity_num,
                    confidence_num,
                    observation_blob,
                    units_cd,
                    end_date,
                    location_cd, 
                    update_date,
                    download_date,
                    now() import_date,
                    sourcesystem_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                    temp.upload_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                FROM ' || upload_temptable_name_c || ' temp
                WHERE (temp.patient_num IS NOT NULL AND temp.patient_num::text <> '''') and (temp.encounter_num IS NOT NULL AND temp.encounter_num::text <> '''')
                ';
        ELSE
            EXECUTE '
                UPDATE observation_fact SET valtype_cd = temp.valtype_cd, 
                    tval_char=temp.tval_char, 
                    nval_num = temp.nval_num, 
                    valueflag_cd=temp.valueflag_cd, 
                    quantity_num=temp.quantity_num, 
                    confidence_num=temp.confidence_num, 
                    observation_blob =temp.observation_blob, 
                    units_cd=temp.units_cd, 
                    end_date=temp.end_date, 
                    location_cd =temp.location_cd, 
                    update_date=temp.update_date, 
                    download_date =temp.download_date, 
                    import_date=temp.import_date, 
                    sourcesystem_cd =temp.sourcesystem_cd, 
                    upload_id = temp.upload_id 
                    FROM ' || upload_temptable_name_c || ' temp WHERE coalesce(observation_fact.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) 
                                                                    <= coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) 
                                                                    and temp.encounter_num = observation_fact.encounter_num 
                                                                    and temp.patient_num = observation_fact.patient_num 
                                                                    and temp.concept_cd = observation_fact.concept_cd 
                                                                    and temp.start_date = observation_fact.start_date 
                                                                    and temp.provider_id = observation_fact.provider_id 
                                                                    and temp.modifier_cd = observation_fact.modifier_cd 
                                                                    and temp.instance_num = observation_fact.instance_num';
            EXECUTE '
                INSERT INTO observation_fact (
                    encounter_num, 
                    concept_cd, 
                    patient_num, 
                    provider_id, 
                    start_date, 
                    modifier_cd, 
                    instance_num, 
                    valtype_cd, 
                    tval_char, 
                    nval_num, 
                    valueflag_cd, 
                    quantity_num, 
                    confidence_num, 
                    observation_blob, 
                    units_cd, 
                    end_date, 
                    location_cd, 
                    update_date, 
                    download_date, 
                    import_date, 
                    sourcesystem_cd, 
                    upload_id)
                SELECT temp2.encounter_num, 
                    temp2.concept_cd, 
                    temp2.patient_num, 
                    temp2.provider_id, 
                    temp2.start_date, 
                    temp2.modifier_cd, 
                    temp2.instance_num, 
                    temp2.valtype_cd, 
                    temp2.tval_char, 
                    temp2.nval_num, 
                    temp2.valueflag_cd, 
                    temp2.quantity_num, 
                    temp2.confidence_num, 
                    temp2.observation_blob, 
                    temp2.units_cd, 
                    temp2.end_date, 
                    temp2.location_cd, 
                    temp2.update_date, 
                    temp2.download_date, 
                    temp2.import_date, 
                    temp2.sourcesystem_cd, 
                    temp2.upload_id) 
                FROM ' || upload_temptable_name_c || ' temp2 WHERE (temp2.patient_num IS NOT NULL AND temp2.patient_num::text <> '''') 
                                                                    and (temp2.encounter_num IS NOT NULL AND temp2.encounter_num::text <> '''')
                                                                    and (temp.encounter_num <> observation_fact.encounter_num 
                                                                    or temp.patient_num <> observation_fact.patient_num 
                                                                    or temp.concept_cd <> observation_fact.concept_cd 
                                                                    or temp.start_date <> observation_fact.start_date 
                                                                    or temp.provider_id <> observation_fact.provider_id 
                                                                    or temp.modifier_cd <> observation_fact.modifier_cd 
                                                                    or temp.instance_num <> observation_fact.instance_num)';
        END IF ;
        ANALYZE OBSERVATION_FACT;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM; 
    END;
$body$
LANGUAGE PLPGSQL
;


CREATE OR REPLACE FUNCTION EUREKA.EK_INSERT_EID_MAP_FROMTEMP ( tempEidTableName text, upload_id bigint)  RETURNS VOID AS $body$
DECLARE
        existingEncounterNum bigint;
        maxEncounterNum bigint;
        distinctEidCur REFCURSOR;
        sql_stmt  varchar(400);
        disEncounterId varchar(100); 
        disEncounterIdSource varchar(100);
        patientMapId varchar(200);
        patientMapIdSource varchar(50);
BEGIN
        LOCK TABLE  encounter_mapping IN EXCLUSIVE MODE NOWAIT;
        select max(encounter_num) into STRICT  maxEncounterNum from encounter_mapping; 
        if coalesce(maxEncounterNum::text, '') = '' then 
            maxEncounterNum := 0;
        end if;
        
        sql_stmt := 'SELECT distinct encounter_id, encounter_id_source, patient_map_id, patient_map_id_source from ' || tempEidTableName;
        OPEN distinctEidCur FOR EXECUTE sql_stmt ;
        LOOP
            FETCH distinctEidCur INTO disEncounterId, disEncounterIdSource, patientMapId, patientMapIdSource;
            IF NOT FOUND THEN EXIT; END IF; -- apply on distinctEidCur
            if  disEncounterIdSource = 'HIVE' THEN 
                BEGIN
                    --check if hive number exist, if so assign that number to reset of map_id's within that pid
                    select encounter_num into existingEncounterNum 
                        from encounter_mapping 
                        where encounter_num = cast(disEncounterId as bigint)
                            and encounter_ide_source = 'HIVE';
                 EXCEPTION  
                    when NO_DATA_FOUND THEN
                        existingEncounterNum := null;
                END;
                
                if (existingEncounterNum IS NOT NULL) then 
                    EXECUTE '
                        update ' || tempEidTableName ||' 
                        set encounter_num = cast(encounter_id as bigint), 
                            process_status_flag = ''P''
                        where encounter_id = $1 
                            and encounter_id_source = ''HIVE'' 
                            and not exists (
                                SELECT 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using disEncounterId;
                else 
                    -- generate new patient_num i.e. take max(_num) + 1 
                    if maxEncounterNum < disEncounterId then 
                    maxEncounterNum := disEncounterId;
                    end if ;
                    EXECUTE '
                        update ' || tempEidTableName ||' 
                        set encounter_num = cast(encounter_id as bigint), 
                            process_status_flag = ''P'' 
                        where encounter_id =  $1 
                            and encounter_id_source = ''HIVE'' 
                            and not exists (
                                SELECT 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using disEncounterId;
                end if;    
            else 
                begin
                    select encounter_num into STRICT  existingEncounterNum 
                    from encounter_mapping 
                    where encounter_ide = disEncounterId 
                        and encounter_ide_source = disEncounterIdSource; 
                    -- test if record fetched. 
                 EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        existingEncounterNum := null;
                end;
                if (existingEncounterNum IS NOT NULL) then 
                    EXECUTE '
                        update ' || tempEidTableName ||' 
                        set encounter_num = $1,
                            process_status_flag = ''P''
                        where encounter_id = $2 
                            and encounter_id_source = $3 
                            and not exists (
                                SELECT 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using existingEncounterNum, 
                        disEncounterId, 
                        disEncounterIdSource;
                else 
                    maxEncounterNum := maxEncounterNum + 1 ;
                                 --TODO : add update colunn
                    EXECUTE '
                        insert into ' || tempEidTableName ||' (
                            encounter_map_id,
                            encounter_map_id_source,
                            encounter_id,
                            encounter_id_source,
                            encounter_num,
                            patient_map_id,
                            patient_map_id_source,
                            process_status_flag,
                            encounter_map_id_status,
                            update_date,
                            download_date,
                            import_date,
                            sourcesystem_cd) 
                        values(
                            $1,
                            ''HIVE'',
                            $2,
                            ''HIVE'',
                            $3,
                            $4,
                            $5,
                            ''P'',
                            ''A'',
                            now(),
                            now(),
                            now(),
                            ''edu.harvard.i2b2.crc''
                        )' 
                    using maxEncounterNum,
                        maxEncounterNum,
                        maxEncounterNum,
                        patientMapId,
                        patientMapIdSource; 
                    EXECUTE '
                        update ' || tempEidTableName ||' 
                        set encounter_num =  $1 , 
                            process_status_flag = ''P'' 
                        where encounter_id = $2 
                            and encounter_id_source = $3 
                            and not exists (
                                SELECT 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using maxEncounterNum, disEncounterId, disEncounterIdSource;
                end if;
            end if; 
        END LOOP;
        CLOSE distinctEidCur ;
        
        -- do the mapping update if the update date is old
        EXECUTE '
            UPDATE encounter_mapping SET ENCOUNTER_NUM = cast(temp.encounter_id as bigint),
                patient_ide = temp.patient_map_id,
                patient_ide_source = temp.patient_map_id_source,
                encounter_ide_status = temp.encounter_map_id_status,
                update_date = temp.update_date, 
                download_date = temp.download_date,
                import_date = now(),
                sourcesystem_cd  = temp.sourcesystem_cd, 
                upload_id = ' || upload_id ||'  FROM ' || tempEidTableName ||' temp 
            WHERE temp.encounter_map_id = encounter_mapping.ENCOUNTER_IDE 
                and temp.encounter_map_id_source = encounter_mapping.ENCOUNTER_IDE_SOURCE
                and temp.encounter_id_source = ''HIVE'' 
                and temp.process_status_flag is null 
                and coalesce(encounter_mapping.update_date, to_date(''01-JAN-1900'',''DD-MON-YYYY'')) <= coalesce(temp.update_date, to_date(''01-JAN-1900'',''DD-MON-YYYY''))';
        -- insert new mapping records i.e flagged P -- jk: added project_id
        EXECUTE '
            insert into encounter_mapping (
                encounter_ide,
                encounter_ide_source,
                encounter_ide_status,
                encounter_num,
                patient_ide,
                patient_ide_source,
                update_date,
                download_date,
                import_date,
                sourcesystem_cd,
                project_id,
                upload_id) 
            SELECT encounter_map_id,
                encounter_map_id_source,
                encounter_map_id_status,
                encounter_num,
                patient_map_id,
                patient_map_id_source,
                update_date,
                download_date,
                now(),
                sourcesystem_cd,
                ''@'' project_id,' 
                || upload_id || ' 
            from ' || tempEidTableName || '  
            where process_status_flag = ''P'' '; 
       ANALYZE encounter_mapping;
    EXCEPTION
        WHEN OTHERS THEN
           RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;
    END;
$body$
LANGUAGE PLPGSQL
;




CREATE OR REPLACE FUNCTION EUREKA.EK_INSERT_PID_MAP_FROMTEMP ( tempPidTableName text, upload_id bigint)  RETURNS VOID AS $body$
DECLARE
        existingPatientNum bigint;
        maxPatientNum bigint;
        distinctPidCur REFCURSOR;
        sql_stmt  varchar(400);
        disPatientId varchar(100); 
        disPatientIdSource varchar(100);
BEGIN
        sql_stmt := 'SELECT distinct patient_id,patient_id_source from ' || tempPidTableName ||' ';
        
        LOCK TABLE  patient_mapping IN EXCLUSIVE MODE NOWAIT;
        select max(patient_num) into STRICT  maxPatientNum from patient_mapping ; 
        -- set max patient num to zero of the value is null
        if coalesce(maxPatientNum::text, '') = '' then maxPatientNum := 0;
        end if;
        
        open distinctPidCur for EXECUTE sql_stmt ;
        loop
            FETCH distinctPidCur INTO disPatientId, disPatientIdSource;
            IF NOT FOUND THEN EXIT; END IF; -- apply on distinctPidCur
            if  disPatientIdSource = 'HIVE'  THEN 
                begin
                    --check if hive number exist, if so assign that number to reset of map_id's within that pid
                    select patient_num into existingPatientNum 
                    from patient_mapping 
                    where patient_num = disPatientId 
                        and patient_ide_source = 'HIVE';
                 EXCEPTION  
                    when NO_DATA_FOUND THEN
                        existingPatientNum := null;
                end;
                if (existingPatientNum IS NOT NULL) then 
                    EXECUTE '
                        update ' || tempPidTableName ||' 
                        set patient_num = cast(patient_id as bigint), 
                            process_status_flag = ''P''
                        where patient_id = $1
                            and patient_id_source = ''HIVE'' 
                            and not exists (
                                SELECT 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                    and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using disPatientId;
                else 
                    -- generate new patient_num i.e. take max(patient_num) + 1 
                    if maxPatientNum < disPatientId then 
                        maxPatientNum := disPatientId;
                    end if ;
                    EXECUTE ' 
                        update ' || tempPidTableName ||' 
                        set patient_num = cast(patient_id as bigint), 
                            process_status_flag = ''P'' 
                        where patient_id = $1 
                            and patient_id_source = ''HIVE'' 
                            and not exists (
                                SELECT 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using disPatientId;
                end if;
            else 
                BEGIN
                    select patient_num into STRICT  existingPatientNum 
                    from patient_mapping 
                    where patient_ide = disPatientId and 
                        patient_ide_source = disPatientIdSource ; 
                    -- test if record fetched. 
                 EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        existingPatientNum := null;
                end;
                if (existingPatientNum IS NOT NULL) then 
                    EXECUTE ' 
                        update ' || tempPidTableName ||' 
                        set patient_num = $1, 
                            process_status_flag = ''P''
                        where patient_id = $2 
                            and patient_id_source = $3 
                            and not exists (
                                SELECT 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                    and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using existingPatientNum, disPatientId, disPatientIdSource;
                else 
                    maxPatientNum := maxPatientNum + 1; 
                    EXECUTE '
                        insert into ' || tempPidTableName ||' (
                            patient_map_id,
                            patient_map_id_source,
                            patient_id,
                            patient_id_source,
                            patient_num,
                            process_status_flag,
                            patient_map_id_status,
                            update_date,
                            download_date,
                            import_date,
                            sourcesystem_cd) 
                        values (
                            $1,
                            ''HIVE'',
                            $2,
                            ''HIVE'',
                            $3,
                            ''P'',
                            ''A'',
                            now(),
                            now(),
                            now(),
                            ''edu.harvard.i2b2.crc''
                        )' 
                    using maxPatientNum,maxPatientNum,maxPatientNum; 
                    EXECUTE '
                        update ' || tempPidTableName ||' 
                        set patient_num =  $1,
                            process_status_flag = ''P'' 
                        where patient_id = $2 
                            and patient_id_source = $3 
                            and not exists (
                                SELECT 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                    and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using maxPatientNum, disPatientId, disPatientIdSource;
                end if ;
            end if; 
        END LOOP;
        CLOSE distinctPidCur ;
        
        -- do the mapping update if the update date is old
        EXECUTE '
        	UPDATE patient_mapping SET patient_num = cast(temp.patient_id as bigint),
                patient_ide_status = temp.patient_map_id_status,
                update_date = temp.update_date,
                download_date  = temp.download_date,
                import_date = now(),
                sourcesystem_cd  = temp.sourcesystem_cd,
                upload_id = ' || upload_id ||'  FROM ' || tempPidTableName ||' temp WHERE temp.patient_map_id = patient_mapping.patient_IDE 
                and temp.patient_map_id_source = patient_mapping.patient_IDE_SOURCE
                and temp.process_status_flag is null  
                and coalesce(patient_mapping.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) <= coalesce(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) 
          ';
           -- insert new mapping records i.e flagged P - jk: added project id
        EXECUTE '
            insert into patient_mapping (
                patient_ide,
                patient_ide_source,
                patient_ide_status,
                patient_num,
                update_date,
                download_date,
                import_date,
                sourcesystem_cd,
                project_id,
                upload_id) 
            SELECT patient_map_id,
                patient_map_id_source,
                patient_map_id_status,
                patient_num,
                update_date,
                download_date,
                now(),
                sourcesystem_cd,
                ''@'' project_id,' 
                || upload_id ||' 
            from '|| tempPidTableName || ' 
            where process_status_flag = ''P'' '; 
        ANALYZE patient_mapping;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'An error was encountered - % -ERROR- %',SQLSTATE,SQLERRM;
    END;
$body$
LANGUAGE PLPGSQL
;



CREATE OR REPLACE FUNCTION EUREKA.EK_DISABLE_INDEXES () RETURNS VOID AS $body$
    BEGIN
 
    END;
$body$
LANGUAGE PLPGSQL
;


CREATE OR REPLACE FUNCTION EUREKA.EK_ENABLE_INDEXES () RETURNS VOID AS $body$
    BEGIN
 
    END;
$body$
LANGUAGE PLPGSQL
;
