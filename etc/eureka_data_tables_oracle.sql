CREATE TABLE EK_TEMP_CONCEPT
  (
    CONCEPT_CD   VARCHAR2(50) NOT NULL,
    CONCEPT_PATH VARCHAR2(900) NOT NULL ,
    NAME_CHAR    VARCHAR2(2000),
    CONCEPT_BLOB CLOB,
    UPDATE_DATE     DATE,
    DOWNLOAD_DATE   DATE,
    IMPORT_DATE     DATE,
    SOURCESYSTEM_CD VARCHAR2(50)
  );
CREATE INDEX EK_IDX_TEMP_CONCEPT_PAT_ID ON EK_TEMP_CONCEPT
  (CONCEPT_PATH
  );
CREATE TABLE EK_TEMP_ENCOUNTER_MAPPING
  (
    ENCOUNTER_MAP_ID        VARCHAR2(200) NOT NULL,
    ENCOUNTER_MAP_ID_SOURCE VARCHAR2(50) NOT NULL,
    PATIENT_MAP_ID          VARCHAR2(200),
    PATIENT_MAP_ID_SOURCE   VARCHAR2(50),
    ENCOUNTER_ID            VARCHAR2(200) NOT NULL,
    ENCOUNTER_ID_SOURCE     VARCHAR2(50) ,
    ENCOUNTER_NUM           NUMBER,
    ENCOUNTER_MAP_ID_STATUS VARCHAR2(50),
    PROCESS_STATUS_FLAG     CHAR(1),
    UPDATE_DATE             DATE,
    DOWNLOAD_DATE           DATE,
    IMPORT_DATE             DATE,
    SOURCESYSTEM_CD         VARCHAR2(50)
  );
CREATE INDEX EK_IDX_TEMP_ENCMAP_EID_ID ON EK_TEMP_ENCOUNTER_MAPPING
  (
    ENCOUNTER_ID,
    ENCOUNTER_ID_SOURCE,
    ENCOUNTER_MAP_ID,
    ENCOUNTER_MAP_ID_SOURCE,
    ENCOUNTER_NUM
  );
CREATE INDEX EK_IDX_TEMP_ENCMAP_STAT_EID_ID ON EK_TEMP_ENCOUNTER_MAPPING
  (
    PROCESS_STATUS_FLAG
  );
CREATE TABLE EK_TEMP_MODIFIER
  (
    MODIFIER_CD   VARCHAR2(50) NOT NULL,
    MODIFIER_PATH VARCHAR2(900) NOT NULL ,
    NAME_CHAR     VARCHAR2(2000),
    MODIFIER_BLOB CLOB,
    UPDATE_DATE     DATE,
    DOWNLOAD_DATE   DATE,
    IMPORT_DATE     DATE,
    SOURCESYSTEM_CD VARCHAR2(50)
  );
CREATE INDEX EK_IDX_TEMP_MODIFIER_PAT_ID ON EK_TEMP_MODIFIER
  (MODIFIER_PATH
  );
CREATE TABLE EK_TEMP_PATIENT
  (
    PATIENT_ID        VARCHAR2(200),
    PATIENT_ID_SOURCE VARCHAR2(50),
    PATIENT_NUM       NUMBER(38,0),
    VITAL_STATUS_CD   VARCHAR2(50),
    BIRTH_DATE        DATE,
    DEATH_DATE        DATE,
    SEX_CD            VARCHAR2(50),
    AGE_IN_YEARS_NUM  NUMBER(5,0),
    LANGUAGE_CD       VARCHAR2(50),
    RACE_CD           VARCHAR2(50),
    MARITAL_STATUS_CD VARCHAR2(50),
    RELIGION_CD       VARCHAR2(50),
    ZIP_CD            VARCHAR2(50),
    STATECITYZIP_PATH VARCHAR2(700),
    PATIENT_BLOB CLOB,
    UPDATE_DATE     DATE,
    DOWNLOAD_DATE   DATE,
    IMPORT_DATE     DATE,
    SOURCESYSTEM_CD VARCHAR2(50)
  );
CREATE INDEX EK_IDX_TEMP_PATIENT_PAT_ID ON EK_TEMP_PATIENT
  (
    PATIENT_ID,
    PATIENT_ID_SOURCE,
    PATIENT_NUM
  );
CREATE TABLE EK_TEMP_PATIENT_MAPPING
  (
    PATIENT_MAP_ID        VARCHAR2(200),
    PATIENT_MAP_ID_SOURCE VARCHAR2(50),
    PATIENT_ID_STATUS     VARCHAR2(50),
    PATIENT_ID            VARCHAR2(200),
    PATIENT_ID_SOURCE     VARCHAR(50),
    PATIENT_NUM           NUMBER(38,0),
    PATIENT_MAP_ID_STATUS VARCHAR2(50),
    PROCESS_STATUS_FLAG   CHAR(1),
    UPDATE_DATE           DATE,
    DOWNLOAD_DATE         DATE,
    IMPORT_DATE           DATE,
    SOURCESYSTEM_CD       VARCHAR2(50)
  );
CREATE INDEX EK_IDX_TEMP_PATMAP_PID_ID ON EK_TEMP_PATIENT_MAPPING
  (
    PATIENT_ID,
    PATIENT_ID_SOURCE
  );
CREATE INDEX EK_IDX_TEMP_PATMAP_MAP_PID_ID ON EK_TEMP_PATIENT_MAPPING
  (
    PATIENT_ID,
    PATIENT_ID_SOURCE,
    PATIENT_MAP_ID,
    PATIENT_MAP_ID_SOURCE,
    PATIENT_NUM
  );
CREATE INDEX EK_IDX_TEMP_PATMAP_STAT_PID_ID ON EK_TEMP_PATIENT_MAPPING
  (
    PROCESS_STATUS_FLAG
  );
CREATE TABLE EK_TEMP_PROVIDER
  (
    PROVIDER_ID   VARCHAR2(50) NOT NULL,
    PROVIDER_PATH VARCHAR2(700) NOT NULL,
    NAME_CHAR     VARCHAR2(2000),
    PROVIDER_BLOB CLOB,
    UPDATE_DATE     DATE,
    DOWNLOAD_DATE   DATE,
    IMPORT_DATE     DATE,
    SOURCESYSTEM_CD VARCHAR2(50),
    UPLOAD_ID       NUMBER(*,0)
  );
CREATE INDEX EK_IDX_TEMP_PROVIDER_PPATH_ID ON EK_TEMP_PROVIDER
  (PROVIDER_PATH
  );
CREATE TABLE EK_TEMP_OBSERVATION
  (
    encounter_num       NUMBER(38,0),
    encounter_id        VARCHAR(200) NOT NULL,
    encounter_id_source VARCHAR(50) NOT NULL,
    concept_cd          VARCHAR(50) NOT NULL,
    patient_num         NUMBER(38,0),
    patient_id          VARCHAR(200) NOT NULL,
    patient_id_source   VARCHAR(50) NOT NULL,
    provider_id         VARCHAR(50),
    start_date          DATE,
    modifier_cd         VARCHAR2(100),
    instance_num        NUMBER(18,0),
    valtype_cd          VARCHAR2(50),
    tval_char           VARCHAR(255),
    nval_num            NUMBER(18,5),
    valueflag_cd        CHAR(50),
    quantity_num        NUMBER(18,5),
    confidence_num      NUMBER(18,0),
    observation_blob CLOB,
    units_cd        VARCHAR2(50),
    end_date        DATE,
    location_cd     VARCHAR2(50),
    update_date     DATE,
    download_date   DATE,
    import_date     DATE,
    sourcesystem_cd VARCHAR2(50) ,
    upload_id       INTEGER
  )
  NOLOGGING;
CREATE TABLE EK_TEMP_OBSERVATION_COMPLETE
  (
    encounter_num       NUMBER(38,0),
    encounter_id        VARCHAR(200) NOT NULL,
    encounter_id_source VARCHAR(50) NOT NULL,
    concept_cd          VARCHAR(50) NOT NULL,
    patient_num         NUMBER(38,0),
    patient_id          VARCHAR(200) NOT NULL,
    patient_id_source   VARCHAR(50) NOT NULL,
    provider_id         VARCHAR(50),
    start_date          DATE,
    modifier_cd         VARCHAR2(100),
    instance_num        NUMBER(18,0),
    valtype_cd          VARCHAR2(50),
    tval_char           VARCHAR(255),
    nval_num            NUMBER(18,5),
    valueflag_cd        CHAR(50),
    quantity_num        NUMBER(18,5),
    confidence_num      NUMBER(18,0),
    observation_blob CLOB,
    units_cd        VARCHAR2(50),
    end_date        DATE,
    location_cd     VARCHAR2(50),
    update_date     DATE,
    download_date   DATE,
    import_date     DATE,
    sourcesystem_cd VARCHAR2(50) ,
    upload_id       INTEGER
  )
  NOLOGGING;
CREATE INDEX EK_IDX_TEMP_OBX_COM_PK ON EK_TEMP_OBSERVATION_COMPLETE
  (
    encounter_num,
    patient_num,
    concept_cd,
    provider_id,
    start_date,
    modifier_cd,
    instance_num
  );
CREATE INDEX EK_IDX_TEMP_OBX_COM_ENC_PAT_ID ON EK_TEMP_OBSERVATION_COMPLETE
  (
    encounter_id,
    encounter_id_source,
    patient_id,
    patient_id_source
  );
CREATE TABLE EK_TEMP_VISIT
  (
    encounter_id        VARCHAR(200) NOT NULL,
    encounter_id_source VARCHAR(50) NOT NULL,
    patient_id          VARCHAR(200) NOT NULL,
    patient_id_source   VARCHAR2(50) NOT NULL,
    encounter_num       NUMBER(38,0),
    inout_cd            VARCHAR(50),
    location_cd         VARCHAR2(50),
    location_path       VARCHAR2(900),
    start_date          DATE,
    end_date            DATE,
    visit_blob CLOB,
    update_date     DATE,
    download_date   DATE,
    import_date     DATE,
    sourcesystem_cd VARCHAR2(50),
    active_status_cd VARCHAR2(50),
    length_of_stay NUMBER(38,0)
  );
CREATE INDEX EK_IDX_TEMP_VISIT_ENC_ID ON EK_TEMP_VISIT
  (
    encounter_id,
    encounter_id_source,
    patient_id,
    patient_id_source
  );
CREATE INDEX EK_IDX_TEMP_VISIT_PATIENT_ID ON EK_TEMP_VISIT
  (
    patient_id,
    patient_id_source
  );
CREATE TABLE EK_REJECTED_OBSERVATION_FACT
  (
    encounter_num       NUMBER(38,0),
    encounter_id        VARCHAR(200) NOT NULL,
    encounter_id_source VARCHAR(50) NOT NULL,
    concept_cd          VARCHAR(50) NOT NULL,
    patient_num         NUMBER(38,0),
    patient_id          VARCHAR(200) NOT NULL,
    patient_id_source   VARCHAR(50) NOT NULL,
    provider_id         VARCHAR(50),
    start_date          DATE,
    modifier_cd         VARCHAR2(100),
    instance_num        NUMBER(18,0),
    valtype_cd          VARCHAR2(50),
    tval_char           VARCHAR(255),
    nval_num            NUMBER(18,5),
    valueflag_cd        CHAR(50),
    quantity_num        NUMBER(18,5),
    confidence_num      NUMBER(18,0),
    observation_blob CLOB,
    units_cd        VARCHAR2(50),
    end_date        DATE,
    location_cd     VARCHAR2(50),
    update_date     DATE,
    download_date   DATE,
    import_date     DATE,
    sourcesystem_cd VARCHAR2(50) ,
    upload_id       INTEGER ,
    reason          VARCHAR2(255)
  )
  NOLOGGING;