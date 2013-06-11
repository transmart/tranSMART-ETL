CREATE OR REPLACE FUNCTION "CZ_WRITE_INFO"
(
	jobId NUMERIC,
	messageID NUMERIC ,
	messageLine NUMERIC,
	messageProcedure varchar ,
	infoMessage varchar
)
AS $$

BEGIN

	insert into cz_job_message
    (
      job_id,
      message_id,
      message_line,
      message_procedure,
      info_message,
      seq_id
    )
	select
      jobID,
      messageID,
      messageLine,
      messageProcedure,
      infoMessage,
      max(seq_id)
  from
    cz_job_audit
  where
    job_id = jobID;
  
  COMMIT;

END;

$$ LANGUAGE plpgsql
security definer
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;