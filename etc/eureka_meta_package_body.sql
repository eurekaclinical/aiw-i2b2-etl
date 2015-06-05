CREATE OR REPLACE PACKAGE BODY EUREKA
AS
    PROCEDURE EK_CLEAR_C_TOTALNUM(
        tableName IN VARCHAR)
    IS 
    BEGIN
        EXECUTE IMMEDIATE 'UPDATE ' || tableName || ' SET C_TOTALNUM=NULL';
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_CLEAR_C_TOTALNUM;
    
    PROCEDURE EK_UPDATE_C_TOTALNUM(
        tableName IN VARCHAR)
    IS 
    BEGIN
        EXECUTE IMMEDIATE 'UPDATE ' || tableName || ' a3
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
            ROLLBACK;
            raise_application_error(-20001,'An error was encountered - '||SQLCODE||' -ERROR- '||SQLERRM);       
    END EK_UPDATE_C_TOTALNUM;
END EUREKA ;
