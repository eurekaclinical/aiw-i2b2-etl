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
CREATE OR REPLACE PACKAGE BODY EUREKA
AS
    PROCEDURE EK_INS_PROVIDER_FROMTEMP(
        tempProviderTableName IN VARCHAR, 
        upload_id IN NUMBER)
    IS 
    BEGIN
        execute immediate '
            MERGE INTO provider_dimension
            USING ' || tempProviderTableName || ' temp
            ON (
                temp.provider_path = provider_dimension.provider_path 
            )
            WHEN MATCHED THEN
                UPDATE SET provider_id=temp.provider_id,
                    name_char = temp.name_char,
                    provider_blob = temp.provider_blob,
                    update_date = temp.update_date,
                    download_date = temp.download_date,
                    import_date = sysdate,
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || '
                WHERE nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) >= nvl(provider_dimension.update_date,to_date(''19000101'',''YYYYMMDD''))
            WHEN NOT MATCHED THEN
                INSERT (
                    provider_id,
                    provider_path,
                    name_char,
                    provider_blob,
                    update_date,
                    download_date,
                    import_date,
                    sourcesystem_cd,
                    upload_id)
                VALUES (
                    temp.provider_id,
                    temp.provider_path,
                    temp.name_char,
                    temp.provider_blob,
                    temp.update_date,
                    temp.download_date,
                    sysdate,
                    temp.sourcesystem_cd, '
                    || upload_id ||
                ')';
        dbms_stats.gather_table_stats(USER, 'provider_dimension');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_INS_PROVIDER_FROMTEMP;

    PROCEDURE EK_INS_PATIENT_FROMTEMP(
        tempTableName IN VARCHAR,
        upload_id IN NUMBER)
    IS
        maxPatientNum NUMBER; 
    BEGIN 
        LOCK TABLE patient_mapping IN EXCLUSIVE MODE NOWAIT;
        
        -- Create new patient(patient_mapping) if temp table patient_ide does not exists 
        -- in patient_mapping table.
        execute immediate '
            insert into patient_mapping (
                patient_ide,
                patient_ide_source,
                patient_num,
                patient_ide_status,
                upload_id)
            select temp.patient_id, 
                temp.patient_id_source, 
                temp.patient_id, 
                ''A'',
                '|| upload_id ||'
            from ' || tempTableName || ' temp 
            WHERE NOT EXISTS (
                select patient_ide 
                from patient_mapping pm 
                where pm.patient_ide = temp.patient_id 
                    and pm.patient_ide_source = temp.patient_id_source)
            and temp.patient_id_source = ''HIVE'' ';
        COMMIT;

        -- update patient_num for temp table
        execute immediate '
            MERGE INTO ' || tempTableName || '
            USING patient_mapping
            ON (
                patient_mapping.patient_ide = '|| tempTableName ||'.patient_id
                and patient_mapping.patient_ide_source = '|| tempTableName ||'.patient_id_source
            )
            WHEN MATCHED THEN
                UPDATE SET patient_num = patient_mapping.patient_num';
        COMMIT;

        execute immediate '
            MERGE INTO patient_dimension
            USING ' || tempTableName || ' temp 
            ON (
                temp.patient_num = patient_dimension.patient_num 
            )
            WHEN MATCHED THEN
                UPDATE SET vital_status_cd = temp.vital_status_cd, 
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
                    import_date = sysdate,
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || '
                WHERE nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) >= nvl(patient_dimension.update_date,to_date(''19000101'',''YYYYMMDD''))
            WHEN NOT MATCHED THEN 
                INSERT (
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
                VALUES(temp.patient_num,
                temp.VITAL_STATUS_CD, 
                temp.BIRTH_DATE, 
                temp.DEATH_DATE,
                temp.SEX_CD, 
                temp.AGE_IN_YEARS_NUM,
                temp.LANGUAGE_CD,
                temp.RACE_CD,
                temp.MARITAL_STATUS_CD, 
                temp.RELIGION_CD,
                temp.ZIP_CD,
                temp.STATECITYZIP_PATH,
                temp.PATIENT_BLOB,
                temp.update_date,
                temp.download_date,
                sysdate, 
                temp.sourcesystem_cd,
                '|| UPLOAD_ID || ')
                WHERE temp.patient_num IS NOT NULL';        
        dbms_stats.gather_table_stats(USER, 'patient_dimension');
    EXCEPTION
        WHEN OTHERS THEN
            rollback;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_INS_PATIENT_FROMTEMP;

    PROCEDURE EK_INS_CONCEPT_FROMTEMP(
        tempConceptTableName IN VARCHAR, 
        upload_id IN NUMBER) 
    IS 
    BEGIN 
        execute immediate '
            MERGE INTO concept_dimension
            USING ' || tempConceptTableName || ' temp 
            ON (
                temp.concept_path = concept_dimension.concept_path 
            )
            WHEN MATCHED THEN
                UPDATE SET concept_cd = temp.concept_cd,
                    name_char = temp.name_char,
                    concept_blob = temp.concept_blob,
                    update_date = temp.update_date,
                    download_date = temp.DOWNLOAD_DATE,
                    import_date = sysdate,
                    sourcesystem_cd = temp.SOURCESYSTEM_CD,
                    upload_id = ' || upload_id || '
                WHERE nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) >= nvl(concept_dimension.update_date,to_date(''19000101'',''YYYYMMDD''))
            WHEN NOT MATCHED THEN
                INSERT (
                    concept_cd,
                    concept_path,
                    name_char,
                    concept_blob,
                    update_date,
                    download_date,
                    import_date,
                    sourcesystem_cd,
                    upload_id)
                VALUES (
                    temp.concept_cd,
                    temp.concept_path,
                    temp.name_char,
                    temp.concept_blob,
                    temp.update_date,
                    temp.download_date,
                    sysdate,
                    temp.sourcesystem_cd, '
                    || UPLOAD_ID ||
                ')';
        dbms_stats.gather_table_stats(USER, 'concept_dimension');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_INS_CONCEPT_FROMTEMP;
    
    PROCEDURE EK_INS_MODIFIER_FROMTEMP(
        tempModifierTableName IN VARCHAR, 
        upload_id IN NUMBER) 
    IS 
    BEGIN 
        execute immediate '
            MERGE INTO modifier_dimension 
            USING ' || tempModifierTableName || ' temp
            ON (
                temp.modifier_path = modifier_dimension.modifier_path 
            )
            WHEN MATCHED THEN
                UPDATE SET modifier_cd = temp.modifier_cd,
                    name_char = temp.name_char,
                    modifier_blob = temp.modifier_blob,
                    update_date = temp.update_date,
                    download_date = temp.download_date,
                    import_date = sysdate,
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || '
                WHERE nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) >= nvl(modifier_dimension.update_date,to_date(''19000101'',''YYYYMMDD''))
            WHEN NOT MATCHED THEN 
                INSERT (
                    modifier_cd,
                    modifier_path,
                    name_char,
                    modifier_blob,
                    update_date,
                    download_date,
                    import_date,
                    sourcesystem_cd,
                    upload_id)
                VALUES (
                    temp.modifier_cd,
                    temp.modifier_path,
                    temp.name_char,
                    temp.modifier_blob,
                    temp.update_date,
                    temp.download_date,
                    sysdate,
                    temp.sourcesystem_cd, '
                    || upload_id ||
                ')';
        dbms_stats.gather_table_stats(USER, 'modifier_dimension');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_INS_MODIFIER_FROMTEMP;

    PROCEDURE EK_INS_ENC_VISIT_FROMTEMP(
        tempTableName IN VARCHAR,
        upload_id     IN NUMBER)
    IS
    BEGIN
        LOCK TABLE encounter_mapping IN EXCLUSIVE MODE NOWAIT ;
        --Create new encounter(encounter_mapping) if temp table encounter_ide does not exists
        -- in encounter_mapping table. -- jk added project id
        EXECUTE immediate ' 
            insert into encounter_mapping (
                encounter_ide,
                encounter_ide_source,
                encounter_num,
                patient_ide,
                patient_ide_source,
                encounter_ide_status,
                project_id,upload_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            select temp.encounter_id, 
                temp.encounter_id_source, 
                temp.encounter_id,
                temp.patient_id,
                temp.patient_id_source,
                ''A'',
                ''@'' project_id, 
                ' || upload_id || '                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            from ' || tempTableName || ' temp                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            where not exists (
                select em.encounter_ide 
                from encounter_mapping em 
                where em.encounter_ide = temp.encounter_id 
                    and em.encounter_ide_source = temp.encounter_id_source
                    and em.patient_ide = temp.patient_id
                    and em.patient_ide_source = temp.patient_id_source)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
            and encounter_id_source = ''HIVE'' ';

        -- update encounter_num for temp table
        EXECUTE immediate '
            MERGE INTO ' || tempTableName || ' tempTableName
            USING encounter_mapping em
            ON (
                em.encounter_ide = tempTableName.encounter_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
                and em.encounter_ide_source = tempTableName.encounter_id_source                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                and nvl(em.patient_ide_source,'''') = nvl(tempTableName.patient_id_source,'''')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
                and nvl(em.patient_ide,'''') = nvl(tempTableName.patient_id,'''')     
            )
            WHEN MATCHED THEN
                UPDATE SET encounter_num = em.encounter_num' ;
        
        EXECUTE immediate '
            MERGE INTO visit_dimension
            USING ' || tempTableName || ' temp
            ON (
                temp.encounter_num = visit_dimension.encounter_num                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
            )
            WHEN MATCHED THEN
                UPDATE SET start_date = temp.start_date,
                    end_date = temp.end_date,
                    inout_cd = temp.inout_cd,
                    location_cd = temp.location_cd,
                    visit_blob = temp.visit_blob,
                    update_date = temp.update_date,
                    download_date = temp.download_date,
                    import_date = sysdate,
                    sourcesystem_cd = temp.sourcesystem_cd,
                    upload_id = ' || upload_id || ',
                    length_of_stay = temp.length_of_stay
                    WHERE nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) >= nvl(visit_dimension.update_date,to_date(''19000101'',''YYYYMMDD''))';
            
        -- jk: added project_id='@' to WHERE clause... need to support projects...
        EXECUTE immediate '
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
                sysdate,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                temp.sourcesystem_cd, '
                || upload_id || ',
                temp.LENGTH_OF_STAY
            from ' || tempTableName || '  temp, patient_mapping pm                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
            where temp.encounter_num is not null 
            and temp.encounter_num not in (
                select encounter_num 
                from visit_dimension vd
            ) 
            and pm.patient_ide = temp.patient_id 
            and pm.patient_ide_source = temp.patient_id_source';
        dbms_stats.gather_table_stats(USER, 'visit_dimension');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK ;
            raise_application_error ( -20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM ) ;
    END EK_INS_ENC_VISIT_FROMTEMP ;

    PROCEDURE EK_UPDATE_OBSERVATION_FACT(
        upload_temptable_name IN VARCHAR,
        upload_temptable_name_c IN VARCHAR,
        upload_id             IN NUMBER,
        appendFlag            IN NUMBER)
    IS
    BEGIN
        EXECUTE IMMEDIATE 'INSERT INTO ' || upload_temptable_name_c || '
            (encounter_num, 
                encounter_id, 
                encounter_id_source, 
                concept_cd, 
                patient_num, 
                patient_id, 
                patient_id_source, 
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
        COMMIT;
        
        EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || upload_temptable_name;

        -- if the append is true, then do the update else do insert all
        IF ( appendFlag = 0 ) THEN
            --Transfer all rows from temp_obsfact to observation_fact
            EXECUTE immediate
                'INSERT ALL INTO observation_fact(encounter_num,concept_cd, patient_num,provider_id, start_date,modifier_cd,instance_num,valtype_cd,tval_char,nval_num,valueflag_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                    quantity_num,confidence_num,observation_blob,units_cd,end_date,location_cd, update_date,download_date,import_date,sourcesystem_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                    upload_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                    SELECT encounter_num,concept_cd, patient_num,provider_id, start_date,modifier_cd,instance_num,valtype_cd,tval_char,nval_num,valueflag_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                    quantity_num,confidence_num,observation_blob,units_cd,end_date,location_cd, update_date,download_date,sysdate import_date,sourcesystem_cd,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                    temp.upload_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
                    FROM ' || upload_temptable_name_c || ' temp
                    where temp.patient_num is not null and  temp.encounter_num is not null'
                    ;
        ELSE
            EXECUTE immediate
                'MERGE INTO observation_fact USING ' || upload_temptable_name_c || ' temp ON 
                    (temp.encounter_num = observation_fact.encounter_num 
                    and temp.patient_num = observation_fact.patient_num 
                    and temp.concept_cd = observation_fact.concept_cd 
                    and temp.start_date = observation_fact.start_date 
                    and temp.provider_id = observation_fact.provider_id 
                    and temp.modifier_cd = observation_fact.modifier_cd 
                    and temp.instance_num = observation_fact.instance_num) 
                when matched then 
                    update set valtype_cd = temp.valtype_cd, 
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
                    where nvl(observation_fact.update_date,to_date(''19000101'',''YYYYMMDD'')) <= nvl(temp.update_date,to_date(''19000101'',''YYYYMMDD'')) 
                when not matched then 
                    insert (encounter_num, 
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
                    values (temp.encounter_num, 
                        temp.concept_cd, 
                        temp.patient_num, 
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
                        temp.upload_id) 
                    where temp.patient_num is not null and temp.encounter_num is not null'
                ;
        END IF ;
        dbms_stats.gather_table_stats(USER, 'OBSERVATION_FACT');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM ) ;
    END EK_UPDATE_OBSERVATION_FACT ;

    PROCEDURE EK_INSERT_EID_MAP_FROMTEMP (
        tempEidTableName IN VARCHAR,
        upload_id IN NUMBER) 
    IS
        existingEncounterNum varchar2(32);
        maxEncounterNum number;
        TYPE distinctEIdCurTyp IS REF CURSOR;
        distinctEidCur   distinctEIdCurTyp;
        sql_stmt  varchar2(400);
        disEncounterId varchar2(100); 
        disEncounterIdSource varchar2(100);
        patientMapId varchar2(200);
        patientMapIdSource varchar2(50);
    BEGIN
        LOCK TABLE  encounter_mapping IN EXCLUSIVE MODE NOWAIT;
        select max(encounter_num) into maxEncounterNum from encounter_mapping; 
        
        if maxEncounterNum is null then 
            maxEncounterNum := 0;
        end if;

        sql_stmt := 'SELECT distinct encounter_id, encounter_id_source, patient_map_id, patient_map_id_source from ' || tempEidTableName;
        OPEN distinctEidCur FOR sql_stmt ;
        LOOP
            FETCH distinctEidCur INTO disEncounterId, disEncounterIdSource, patientMapId, patientMapIdSource;
            EXIT WHEN distinctEidCur%NOTFOUND;
            if  disEncounterIdSource = 'HIVE' THEN 
                BEGIN
                    --check if hive number exist, if so assign that number to reset of map_id's within that pid
                    select encounter_num into existingEncounterNum 
                        from encounter_mapping 
                        where encounter_num = disEncounterId 
                            and encounter_ide_source = 'HIVE';
                EXCEPTION  
                    when NO_DATA_FOUND THEN
                        existingEncounterNum := null;
                END;
                if existingEncounterNum is not null then 
                    execute immediate '
                        update ' || tempEidTableName ||' 
                        set encounter_num = encounter_id, 
                            process_status_flag = ''P''
                        where encounter_id = :x 
                            and encounter_id_source = ''HIVE'' 
                            and not exists (
                                select 1 
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
                    execute immediate '
                        update ' || tempEidTableName ||' 
                        set encounter_num = encounter_id, 
                            process_status_flag = ''P'' 
                        where encounter_id =  :x 
                            and encounter_id_source = ''HIVE'' 
                            and not exists (
                                select 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using disEncounterId;
                end if;    
            else 
                begin
                    select encounter_num into existingEncounterNum 
                    from encounter_mapping 
                    where encounter_ide = disEncounterId 
                        and encounter_ide_source = disEncounterIdSource; 
                    -- test if record fetched. 
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        existingEncounterNum := null;
                end;
                if existingEncounterNum is not null then 
                    execute immediate '
                        update ' || tempEidTableName ||' 
                        set encounter_num = :x,
                            process_status_flag = ''P''
                        where encounter_id = :y 
                            and encounter_id_source = :z 
                            and not exists (
                                select 1 
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
                    execute immediate '
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
                            :x,
                            ''HIVE'',
                            :y,
                            ''HIVE'',
                            :z,
                            :w,
                            :v,
                            ''P'',
                            ''A'',
                            sysdate,
                            sysdate,
                            sysdate,
                            ''edu.harvard.i2b2.crc''
                        )' 
                    using maxEncounterNum,
                        maxEncounterNum,
                        maxEncounterNum,
                        patientMapId,
                        patientMapIdSource; 
                    
                    execute immediate '
                        update ' || tempEidTableName ||' 
                        set encounter_num =  :x , 
                            process_status_flag = ''P'' 
                        where encounter_id = :y 
                            and encounter_id_source = :z 
                            and not exists (
                                select 1 
                                from encounter_mapping em 
                                where em.encounter_ide = encounter_map_id
                                    and em.encounter_ide_source = encounter_map_id_source
                            )' 
                    using maxEncounterNum, disEncounterId, disEncounterIdSource;
                end if;
            end if; 
        END LOOP;
        CLOSE distinctEidCur ;
        COMMIT;
        
        -- do the mapping update if the update date is old
        execute immediate '
            merge into encounter_mapping
            using ' || tempEidTableName ||' temp
            on (temp.encounter_map_id = encounter_mapping.ENCOUNTER_IDE 
                and temp.encounter_map_id_source = encounter_mapping.ENCOUNTER_IDE_SOURCE
            ) 
            when matched then 
                update 
                set ENCOUNTER_NUM = temp.encounter_id,
                    patient_ide = temp.patient_map_id,
                    patient_ide_source = temp.patient_map_id_source,
                    encounter_ide_status = temp.encounter_map_id_status,
                    update_date = temp.update_date, 
                    download_date = temp.download_date,
                    import_date = sysdate,
                    sourcesystem_cd  = temp.sourcesystem_cd, 
                    upload_id = ' || upload_id ||'  
                where temp.encounter_id_source = ''HIVE'' 
                    and temp.process_status_flag is null 
                    and nvl(encounter_mapping.update_date, to_date(''01-JAN-1900'',''DD-MON-YYYY'')) <= nvl(temp.update_date, to_date(''01-JAN-1900'',''DD-MON-YYYY''))';

        -- insert new mapping records i.e flagged P -- jk: added project_id
        execute immediate '
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
            select encounter_map_id,
                encounter_map_id_source,
                encounter_map_id_status,
                encounter_num,
                patient_map_id,
                patient_map_id_source,
                update_date,
                download_date,
                sysdate,
                sourcesystem_cd,
                ''@'' project_id,' 
                || upload_id || ' 
            from ' || tempEidTableName || '  
            where process_status_flag = ''P'' '; 
        dbms_stats.gather_table_stats(USER, 'encounter_mapping');
    EXCEPTION
        WHEN OTHERS THEN
           if distinctEidCur%isopen then
               close distinctEidCur;
           end if;
           rollback;
           raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
    END EK_INSERT_EID_MAP_FROMTEMP;

    PROCEDURE EK_INSERT_PID_MAP_FROMTEMP (
        tempPidTableName IN VARCHAR,
        upload_id IN NUMBER) 
    IS
        existingPatientNum varchar2(32);
        maxPatientNum number;
        TYPE distinctPidCurTyp IS REF CURSOR;
        distinctPidCur   distinctPidCurTyp;
        sql_stmt  varchar2(400);
        disPatientId varchar2(100); 
        disPatientIdSource varchar2(100);
    BEGIN
        sql_stmt := ' SELECT distinct patient_id,patient_id_source from ' || tempPidTableName ||' ';

        LOCK TABLE  patient_mapping IN EXCLUSIVE MODE NOWAIT;
        select max(patient_num) into maxPatientNum from patient_mapping ; 
        -- set max patient num to zero of the value is null
        if maxPatientNum is null then maxPatientNum := 0;
        end if;

        open distinctPidCur for sql_stmt ;
        loop
            FETCH distinctPidCur INTO disPatientId, disPatientIdSource;
            EXIT WHEN distinctPidCur%NOTFOUND;
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
                if existingPatientNum is not null then 
                    execute immediate '
                        update ' || tempPidTableName ||' 
                        set patient_num = patient_id, 
                            process_status_flag = ''P''
                        where patient_id = :x 
                            and patient_id_source = ''HIVE'' 
                            and not exists (
                                select 1 
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
                    execute immediate ' 
                        update ' || tempPidTableName ||' 
                        set patient_num = patient_id, 
                            process_status_flag = ''P'' 
                        where patient_id = :x 
                            and patient_id_source = ''HIVE'' 
                            and not exists (
                                select 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using disPatientId;
                end if;
            else 
                BEGIN
                    select patient_num into existingPatientNum 
                    from patient_mapping 
                    where patient_ide = disPatientId and 
                        patient_ide_source = disPatientIdSource ; 
                    -- test if record fetched. 
                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        existingPatientNum := null;
                end;
                if existingPatientNum is not null then 
                    execute immediate ' 
                        update ' || tempPidTableName ||' 
                        set patient_num = :x, 
                            process_status_flag = ''P''
                        where patient_id = :y 
                            and patient_id_source = :z 
                            and not exists (
                                select 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                    and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using existingPatientNum, disPatientId, disPatientIdSource;
                else 
                    maxPatientNum := maxPatientNum + 1; 
                    execute immediate '
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
                            :x,
                            ''HIVE'',
                            :y,
                            ''HIVE'',
                            :z,
                            ''P'',
                            ''A'',
                            sysdate,
                            sysdate,
                            sysdate,
                            ''edu.harvard.i2b2.crc''
                        )' 
                    using maxPatientNum,maxPatientNum,maxPatientNum; 
                    execute immediate '
                        update ' || tempPidTableName ||' 
                        set patient_num =  :x,
                            process_status_flag = ''P'' 
                        where patient_id = :y 
                            and patient_id_source = :z 
                            and not exists (
                                select 1 
                                from patient_mapping pm 
                                where pm.patient_ide = patient_map_id
                                    and pm.patient_ide_source = patient_map_id_source
                            )' 
                    using maxPatientNum, disPatientId, disPatientIdSource;
                end if ;
            end if; 
        END LOOP;
        CLOSE distinctPidCur ;
        COMMIT;

        -- do the mapping update if the update date is old
        execute immediate 
            'merge into patient_mapping
                using ' || tempPidTableName ||' temp
                on (
                    temp.patient_map_id = patient_mapping.patient_IDE 
                    and temp.patient_map_id_source = patient_mapping.patient_IDE_SOURCE
                 ) 
                when matched then 
                    update set patient_num = temp.patient_id,
                        patient_ide_status = temp.patient_map_id_status,
                        update_date = temp.update_date,
                        download_date  = temp.download_date,
                        import_date = sysdate,
                        sourcesystem_cd  = temp.sourcesystem_cd,
                        upload_id = ' || upload_id ||'  
                        where temp.patient_id_source = ''HIVE'' 
                            and temp.process_status_flag is null  
                            and nvl(patient_mapping.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) <= nvl(temp.update_date,to_date(''01-JAN-1900'',''DD-MON-YYYY'')) ';

                        -- insert new mapping records i.e flagged P - jk: added project id
        execute immediate '
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
            select patient_map_id,
                patient_map_id_source,
                patient_map_id_status,
                patient_num,
                update_date,
                download_date,
                sysdate,
                sourcesystem_cd,
                ''@'' project_id,' 
                || upload_id ||' 
            from '|| tempPidTableName || ' 
            where process_status_flag = ''P'' '; 
        dbms_stats.gather_table_stats(USER, 'patient_mapping');
    EXCEPTION
        WHEN OTHERS THEN
            if distinctPidCur%isopen then
                close distinctPidCur;
            end if;
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
    END EK_INSERT_PID_MAP_FROMTEMP;

    PROCEDURE EK_DISABLE_INDEXES
    IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_NOLOB UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_PATCON_DATE_PRVD_IDX UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_CNPT_PAT_ENCT_IDX UNUSABLE';
        EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = true';
    EXCEPTION
        WHEN OTHERS THEN
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
    END EK_DISABLE_INDEXES;

    PROCEDURE EK_ENABLE_INDEXES
    IS
    BEGIN
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_NOLOB REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_PATCON_DATE_PRVD_IDX REBUILD';
        EXECUTE IMMEDIATE 'ALTER INDEX FACT_CNPT_PAT_ENCT_IDX REBUILD';
        EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = false';
    EXCEPTION
        WHEN OTHERS THEN
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);
    END EK_ENABLE_INDEXES;
END EUREKA ;
/