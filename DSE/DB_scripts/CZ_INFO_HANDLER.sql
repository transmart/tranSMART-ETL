CREATE OR REPLACE FUNCTION tm_cz.cz_info_handler
(jobid numeric,
 messageid numeric, 
 messageline numeric, 
 messageprocedure character varying, 
 infomessage character varying, 
 stepnumber character varying
 )
  RETURNS integer AS

DECLARE 
	databaseName VARCHAR(100);
	rtnCd	integer;
BEGIN

	select database_name INTO databaseName
  	from tm_cz.cz_job_master 
	where job_id=jobID;
    
  select tm_cz.cz_write_audit( jobID, databaseName, messageProcedure, 'Step contains more details', 0, stepNumber, 'Information' ) into rtnCd;
  select tm_cz.cz_write_info(jobID, messageID, messageLine, messageProcedure, infoMessage ) into rtnCd;

return rtnCd;
  
END;