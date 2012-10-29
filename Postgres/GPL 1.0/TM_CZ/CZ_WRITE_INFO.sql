CREATE OR REPLACE PROCEDURE "CZ_WRITE_INFO" 
(
	jobId IN NUMBER,
	messageID IN NUMBER , 
	messageLine IN NUMBER, 
	messageProcedure IN VARCHAR2 , 
	infoMessage IN VARCHAR2
)
AS

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

