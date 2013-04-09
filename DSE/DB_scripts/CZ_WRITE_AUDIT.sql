CREATE OR REPLACE FUNCTION tm_cz.CZ_WRITE_AUDIT
(
	jobId numeric,
	databaseName character varying , 
	procedureName character varying , 
	stepDesc character varying , 
	recordsManipulated numeric,
	stepNumber numeric,
	stepStatus character varying
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
DECLARE
	lastTime timestamp;
	currTime timestamp;
	elapsedSecs	numeric;
	rtnCd		numeric;
	
BEGIN

	select max(job_date)
    into lastTime
    from tm_cz.cz_job_audit
    where job_id = jobID;
	
	--	clock_timestamp() is the current system time
	
	select clock_timestamp() into currTime;

	elapsedSecs :=	coalesce(((DATE_PART('day', currTime - lastTime) * 24 + 
				   DATE_PART('hour', currTime - lastTime)) * 60 +
				   DATE_PART('minute', currTime - lastTime)) * 60 +
				   DATE_PART('second', currTime - lastTime),0);

	begin
	insert 	into tm_cz.cz_job_audit
	(job_id
	,database_name
 	,procedure_name
 	,step_desc
	,records_manipulated
	,step_number
	,step_status
    ,job_date
    ,time_elapsed_secs
	)
	values(
 		jobId,
		databaseName,
		procedureName,
		stepDesc,
		recordsManipulated,
		stepNumber,
		stepStatus,
		currTime,
		elapsedSecs);
	exception 
	when OTHERS then
		--raise notice 'proc failed state=%  errm=%', SQLSTATE, SQLERRM;
		select tm_cz.cz_write_error(jobId,0,SQLERRML,SQLSTATE,SQLERRM,null) into rtnCd;
		return -16;
	end;
	
	return 1;
END;
$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, pg_temp;
