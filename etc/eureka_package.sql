CREATE OR REPLACE PACKAGE EUREKA
AS
PROCEDURE EK_INS_ENC_VISIT_FROM_TEMP(
    tempTableName IN VARCHAR,
    upload_id     IN NUMBER,
    errorMsg OUT VARCHAR ) ;
    
PROCEDURE EK_UPDATE_OBSERVATION_FACT(
    upload_temptable_name IN VARCHAR,
    upload_id             IN NUMBER,
    appendFlag            IN NUMBER,
    errorMsg OUT VARCHAR ) ;
    
END EUREKA ;

