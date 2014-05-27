CREATE OR REPLACE function tm_cz.czx_write_info
(
	jobId numeric,
	messageID numeric , 
	messageLine numeric, 
	messageProcedure character varying , 
	infoMessage character varying
)
returns numeric
AS $$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************/

declare

	rtnCd	numeric;
	
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
  	exception 
	when OTHERS then
		--raise notice 'proc failed state=%  errm=%', SQLSTATE, SQLERRM;
		select tm_cz.cz_write_error(jobId,'0',SQLERRML,SQLSTATE,SQLERRM,null) into rtnCd;
		return -16;
	end;
	
	return 1;

END;
$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;
