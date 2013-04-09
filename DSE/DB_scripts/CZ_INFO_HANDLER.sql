CREATE OR REPLACE FUNCTION tm_cz.CZ_INFO_HANDLER 
(
	jobId IN NUMERIC,
	messageID IN NUMERIC , 
	messageLine IN NUMERIC, 
	messageProcedure IN CHARACTER VARYING , 
	infoMessage IN CHARACTER VARYING,
        stepNumber IN CHARACTER VARYING
)
returns integer
AS $$
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

$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;
