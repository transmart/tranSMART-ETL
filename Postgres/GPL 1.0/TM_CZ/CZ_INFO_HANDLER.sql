CREATE OR REPLACE PROCEDURE "CZ_INFO_HANDLER" 
(
	jobId IN NUMBER,
	messageID IN NUMBER , 
	messageLine IN NUMBER, 
	messageProcedure IN VARCHAR2 , 
	infoMessage IN VARCHAR2,
  stepNumber IN VARCHAR2
)
AS
  databaseName VARCHAR2(100);
BEGIN

	select 
    database_name INTO databasename
  from 
    cz_job_master 
	where 
    job_id=jobID;
    
  cz_write_audit( jobID, databaseName, messageProcedure, 'Step contains more details', 0, stepNumber, 'Information' );

  cz_write_info(jobID, messageID, messageLine, messageProcedure, infoMessage );
  
END;
