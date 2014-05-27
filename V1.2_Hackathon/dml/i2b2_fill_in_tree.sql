create or replace function tm_cz.i2b2_fill_in_tree
(
  trial_id character varying
 ,input_path character varying
 ,currentJobID numeric default -1
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
Declare
 
	--Audit variables
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			numeric;
	
    TrialID varchar(100);
	auditText varchar(4000);
	root_node varchar(1000);
	node_name varchar(1000);
	curr_node	varchar(1000);
	etlDate		timestamp without time zone;
	root_level	numeric(18,0);
	v_count numeric;
  
  --Get the nodes
  cNodes CURSOR for
    --Trimming off the last node as it would never need to be added.
    select distinct substr(c_fullname, 1,tm_cz.instr(c_fullname,'\',-2,1)) as c_fullname
    from i2b2metadata.i2b2 
    where c_fullname like input_path || '%' escape '`'
	union
	--	add input_path if filling in upper-level nodes only
	select input_path as c_fullname;

  
BEGIN
	TrialID := upper(trial_id);
  
    stepCt := 0;
  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;

  databaseName := 'tm_cz';
  procedureName := 'i2b2_fill_in_tree';
  select clock_timestamp() into etlDate;

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    select tm_cz.czx_start_audit (procedureName, databaseName) into jobID;
  END IF;
  
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Start ' || procedureName,0,stepCt,'Done') into rtnCd;
  
  --	setup root node
  
  select tm_cz.parse_nth_value(input_path, 2, '\') into curr_node;

  select c_hlevel into root_level
  from i2b2metadata.table_access
  where c_name = curr_node;
  
  truncate table tm_wz.wt_folder_nodes;
  
  --start node with the first slash
 
  --Iterate through each node
	FOR r_cNodes in cNodes Loop
		root_node := '\';
		--Determine how many nodes there are
		--Iterate through, Start with 2 as one will be null from the parser
    
		for loop_counter in 1 .. (length(r_cNodes.c_fullname) - coalesce(length(replace(r_cNodes.c_fullname, '\','')),0)) / length('\')
		LOOP
			--Determine Node
			
			curr_node := substr(r_cNodes.c_fullname,1,tm_cz.instr(r_cNodes.c_fullname,'\',-1,loop_counter));	
			if curr_node is not null and curr_node != '\' then
				begin
				insert into tm_wz.wt_folder_nodes
				(folder_path)
				select curr_node
				where not exists (select 1 from tm_wz.wt_folder_nodes x where x.folder_path = curr_node);
				end;
			end if;
		end loop;
	end loop;
	
	select count(*) into rowCt
	from tm_wz.wt_folder_nodes;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Nbr of nodes to be3 added',rowCt,stepCt,'Done') into rtnCd;

	--	bulk insert concept_dimension records

	begin
	insert into i2b2demodata.concept_dimension
	(concept_cd
	,concept_path
	,name_char
	,update_date
	,download_date
	,import_date
	,sourcesystem_cd)
	Select nextval('i2b2demodata.concept_id')::text
		  ,y.folder_path
		  ,tm_cz.parse_nth_value(y.folder_path,length(y.folder_path)-length(replace(y.folder_path,'\','')),'\')
		  ,etlDate
		  ,etlDate
		  ,etlDate
		  ,TrialId
		 -- ,case when tm_cz.parse_nth_value(y.folder_path,length(y.folder_path)-length(replace(y.folder_path,'\','')),'\') < input_path then null else TrialID end
	from (select distinct folder_path from tm_wz.wt_folder_nodes x
		  where not exists
			   (select 1 from i2b2demodata.concept_dimension cd where x.folder_path = cd.concept_path)) y;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	get diagnostics rowCt := ROW_COUNT;	
	end;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted concept for path into I2B2DEMODATA concept_dimension',rowCt,stepCt,'Done') into rtnCd;

	--	bulk insert the i2b2 records
	
	insert into i2b2metadata.i2b2
	(c_hlevel
	,c_fullname
	,c_name
	,c_visualattributes
	,c_synonym_cd
	,c_facttablecolumn
	,c_tablename
	,c_columnname
	,c_dimcode
	,c_tooltip
	,update_date
	,download_date
	,import_date
	,sourcesystem_cd
	,c_basecode
	,c_operator
	,c_columndatatype
	,c_comment
	--,i2b2_id
	,m_applied_path)
    select (length(cd.concept_path) - coalesce(length(replace(cd.concept_path, '\','')),0)) / length('\') - 2 + root_level
		  ,cd.concept_path
		  ,cd.name_char
		  ,'FA'
		  ,'N'
		  ,'CONCEPT_CD'
		  ,'CONCEPT_DIMENSION'
		  ,'CONCEPT_PATH'
		  ,cd.concept_path
		  ,cd.concept_path
		  ,etlDate
		  ,etlDate
		  ,etlDate
		  ,cd.sourcesystem_cd
		  ,cd.concept_cd
		  ,'LIKE'
		  ,'T'
		  ,case when cd.sourcesystem_cd is null then null else 'trial:' || TrialID end 
		--  ,case when TrialID is null then null else 'trial:' || TrialID end 
		--  ,nextval('i2b2metadata.i2b2_id_seq')
		  ,'@'
    from i2b2demodata.concept_dimension cd
    where cd.concept_path in (select distinct folder_path from tm_wz.wt_folder_nodes)
	  and not exists
		  (select 1 from i2b2metadata.i2b2 x
		   where cd.concept_path = x.c_fullname);
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted path into I2B2METADATA i2b2',rowCt,stepCt,'Done') into rtnCd;

	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'End '|| procedureName,0,stepCt,'Done') into rtnCd;
	
      ---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1 THEN
		select tm_cz.czx_end_audit (jobID, 'SUCCESS') into rtnCD;
	END IF;
  
	return 1;

	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;

  
END;

$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, i2b2demodata, i2b2metadata, pg_temp;