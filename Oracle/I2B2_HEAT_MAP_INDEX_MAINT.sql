set define off;
  CREATE OR REPLACE PROCEDURE "I2B2_HEAT_MAP_INDEX_MAINT" 
(
  run_type 			VARCHAR2 := 'DROP'
 ,tablespace_name	varchar2	:= 'INDX'
 ,currentJobID 		NUMBER := null
)
AS
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

  runType	varchar2(100);
  idxExists	number;
  pExists	number;
  localVar	varchar2(20);
  bitmapVar	varchar2(20);
  bitmapCompress	varchar2(20);
  tableSpace	varchar2(50);
  tText		varchar2(2000);
   
  --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  
BEGIN

	runType := upper(run_type);
	tableSpace := upper(tablespace_name);
	
  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;
  procedureName := $$PLSQL_UNIT;

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    cz_start_audit (procedureName, databaseName, jobID);
  END IF;
    	
  stepCt := 0;
  
	--	Determine if heat_map_results is partitioned, if yes, set localVar to local
  	select count(*)
	into pExists
	from all_tables
	where table_name = 'HEAT_MAP_RESULTS'
	  and owner = 'BIOMART'
	  and partitioned = 'YES';
	  
	if pExists = 0 then
		localVar := null;
		bitmapVar := null;
		bitmapCompress := 'compress';
	else 
		localVar := 'local';
		bitmapVar := 'bitmap';
		bitmapCompress := 'nocompress';
	end if;
   
	if runType = 'DROP' then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Start heat_map_results index drop',0,stepCt,'Done');
		--	drop the indexes
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I1';
		  --and owner = 'BIOMART';
		
		if idxExists = 1 then
			execute immediate('drop index BIOMART.HEAT_MAP_RESULTS_I1');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Drop HEAT_MAP_RESULTS_I1',0,stepCt,'Done');
		end if;
		
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I2';
		  --and owner = 'BIOMART';
		
		if idxExists = 1 then
			execute immediate('drop index BIOMART.HEAT_MAP_RESULTS_I2');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Drop HEAT_MAP_RESULTS_I2',0,stepCt,'Done');
		end if;
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I3';
		  --and owner = 'BIOMART';
		
		if idxExists = 1 then
			execute immediate('drop index BIOMART.HEAT_MAP_RESULTS_I3');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Drop HEAT_MAP_RESULTS_I3',0,stepCt,'Done');
		end if;
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I4';
		  --and owner = 'BIOMART';
		
		if idxExists = 1 then
			execute immediate('drop index BIOMART.HEAT_MAP_RESULTS_I4');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Drop HEAT_MAP_RESULTS_I4',0,stepCt,'Done');
		end if;
						
	else
		--	add indexes
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Start HEAT_MAP_RESULTS index create',0,stepCt,'Done');
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I1'
		  and owner = 'BIOMART';
		  
		if idxExists = 0 then
			tText := 'create ' || bitmapVar || ' index BIOMART.HEAT_MAP_RESULTS_I1 on BIOMART.HEAT_MAP_RESULTS(bio_assay_analysis_id) ' || localVar || ' nologging ' || bitmapCompress || ' tablespace "' || tableSpace || '"';
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done');
			execute immediate(tText); 
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Create HEAT_MAP_RESULTS_I1',0,stepCt,'Done');
		end if;
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I2'
		  and owner = 'BIOMART';
		  
		if idxExists = 0 then		
			execute immediate('create ' || bitmapVar || ' index BIOMART.HEAT_MAP_RESULTS_I2 on BIOMART.HEAT_MAP_RESULTS(search_keyword_id) ' || localVar || ' nologging ' || bitmapCompress || ' tablespace "' || tableSpace || '"');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Create HEAT_MAP_RESULTS_I2',0,stepCt,'Done');
		end if;
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I3'
		  and owner = 'BIOMART';
		  
		if idxExists = 0 then		
			execute immediate('create ' || bitmapVar || ' index BIOMART.HEAT_MAP_RESULTS_I3 on BIOMART.HEAT_MAP_RESULTS(probe_id) ' || localVar || ' nologging ' || bitmapCompress || ' tablespace "' || tableSpace || '"');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Create HEAT_MAP_RESULTS_I3',0,stepCt,'Done');
		end if;
				
		select count(*) 
		into idxExists
		from all_indexes
		where table_name = 'HEAT_MAP_RESULTS'
		  and index_name = 'HEAT_MAP_RESULTS_I4'
		  and owner = 'BIOMART';
		  
		if idxExists = 0 then
			execute immediate('create ' || bitmapVar || ' index BIOMART.HEAT_MAP_RESULTS_I4 on BIOMART.HEAT_MAP_RESULTS(bio_marker_id) ' || localVar || ' nologging ' || bitmapCompress || ' tablespace "' || tableSpace || '"');
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'Create HEAT_MAP_RESULTS_I4',0,stepCt,'Done');
		end if;
						
	end if;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End procedure'||procedureName,SQL%ROWCOUNT,stepCt,'Done');
	commit;	

	
    ---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		cz_end_audit (jobID, 'SUCCESS');
	END IF;

	EXCEPTION
	WHEN OTHERS THEN
		--Handle errors.
		cz_error_handler (jobID, procedureName);
		
		--End Proc
		cz_end_audit (jobID, 'FAIL');
end;
 
 
