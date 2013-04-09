CREATE OR REPLACE FUNCTION tm_cz.CZ_WRITE_INFO 
(
	jobId IN NUMERIC,
	messageID IN NUMERIC , 
	messageLine IN NUMERIC, 
	messageProcedure IN CHARACTER VARYING , 
	infoMessage IN CHARACTER VARYING
)
returns numeric
AS $$

BEGIN
        begin
	insert into tm_cz.cz_job_message
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
    tm_cz.cz_job_audit
  where
    job_id = jobID;
    end;
  
  COMMIT;
  return 1;

END;

$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;
