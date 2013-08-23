CREATE OR REPLACE function tm_cz.CZ_INFO_HANDLER
(
	jobId numeric,
	messageID numeric , 
	messageLine numeric, 
	messageProcedure varchar ,
	infoMessage varchar,
  stepNumber varchar
)
returns numeric
AS $$
declare
  databaseName character varying(100);
BEGIN

	select 
    database_name INTO databaseName
  from 
    cz_job_master 
	where 
    job_id=jobID;
    
  select tm_cz.cz_write_audit( jobID, databaseName, messageProcedure, 'Step contains more details', 0, stepNumber, 'Information' );

  select tm_cz.cz_write_info(jobID, messageID, messageLine, messageProcedure, infoMessage );
  
END;
$$ LANGUAGE plpgsql
security definer
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;

