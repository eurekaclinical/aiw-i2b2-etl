CREATE OR REPLACE PACKAGE EUREKA
AS
-- Load new providers into PROVIDER_DIMENSION, and update existing providers.
PROCEDURE EK_INS_PROVIDER_FROMTEMP(
        tempProviderTableName IN VARCHAR, 
        upload_id IN NUMBER,
        errorMsg OUT VARCHAR);

-- Load new patients into PATIENT_DIMENSION, and update existing patients.
PROCEDURE EK_INS_PATIENT_FROMTEMP(
        tempTableName IN VARCHAR,
        upload_id IN NUMBER,
        errorMsg OUT VARCHAR) ;

-- Load new concepts into CONCEPT_DIMENSION, and update existing concepts.
PROCEDURE EK_INS_CONCEPT_FROMTEMP(
        tempConceptTableName IN VARCHAR, 
        upload_id IN NUMBER, 
        errorMsg OUT VARCHAR ) ;

-- Load new modifiers into MODIFIER_DIMENSION, and update existing modifiers.
PROCEDURE EK_INS_MODIFIER_FROM_TEMP(
        tempModifierTableName IN VARCHAR, 
        upload_id IN NUMBER, 
        errorMsg OUT VARCHAR ) ;

PROCEDURE EK_INS_ENC_VISIT_FROM_TEMP(
    tempTableName IN VARCHAR,
    upload_id     IN NUMBER,
    errorMsg OUT VARCHAR ) ;
    
PROCEDURE EK_UPDATE_OBSERVATION_FACT(
    upload_temptable_name IN VARCHAR,
    upload_id             IN NUMBER,
    appendFlag            IN NUMBER,
    errorMsg OUT VARCHAR ) ;

PROCEDURE EK_INSERT_EID_MAP_FROMTEMP(
    tempEidTableName IN VARCHAR,  
    upload_id IN NUMBER,
    errorMsg OUT VARCHAR ) ;

PROCEDURE EK_INSERT_PID_MAP_FROMTEMP (
    tempPidTableName IN VARCHAR, 
    upload_id IN NUMBER, 
    errorMsg OUT VARCHAR ) ;

END EUREKA ;

