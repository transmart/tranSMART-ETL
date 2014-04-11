-- Function: tm_cz.i2b2_move_study(character varying, character varying, numeric)

-- DROP FUNCTION tm_cz.i2b2_move_study(character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION tm_cz.i2b2_move_study
(trial_id character varying
,topNode character varying
,currentjobid numeric)
  RETURNS integer AS
$BODY$
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

	root_node		varchar(2000);
	root_level		integer;
	rtnCd			integer;
	pExists			int;
	TrialId			varchar(100);
	old_path		varchar(1000);
	newPath			varchar(1000);
	new_study_name	varchar(1000);
	topLevel		integer;
	
  
	--Audit variables
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
  
BEGIN
    
	stepCt := 0;
	TrialId := upper(trial_id);
	
	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := 'TM_CZ';
	procedureName := 'I2B2_MOVE_STUDY';
	
	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select tm_cz.czx_start_audit (procedureName, databaseName) into jobId;
	END IF;
	
	--	check if study exists
	
	select count(*) into pExists
	from i2b2metadata.i2b2
	where sourcesystem_cd = TrialId;
	
	if pExists = 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'TrialId ' || TrialId || ' does not exist',rowCt,stepCt,'Done') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCD;	
		return 16;
	end if;
	
	--	get current top node for study
	
	select min(c_fullname) into old_path
	from i2b2metadata.i2b2
	where sourcesystem_cd = TrialId;
	
	--	check that topNode is not null, '' or %
	
	if coalesce(topNode,'') = '' or topNode = '%' then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'topNode is null, empty or %',rowCt,stepCt,'Done') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCD;	
		return 16;
	end if;	
	
	--	check that topNode does not already exist
	
	select count(*) into pExists
	from i2b2metadata.i2b2
	where c_fullname = topNode;
	
	if pExists > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'topNode already exists',rowCt,stepCt,'Done') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCD;	
		return 16;
	end if;	
	
	--	check topNode for enough nodes
	
	newPath := REGEXP_REPLACE('\' || topNode || '\','(\\){2,}', '\', 'g');
	select length(newPath)-length(replace(newPath,'\','')) into topLevel;
	
	if topLevel < 3 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'topNode does not contain enough nodes',rowCt,stepCt,'Done') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCD;	
		return 16;
	end if;  
  
  	--	get root_node of new path
	
	select tm_cz.parse_nth_value(newPath, 2, '\') into root_node;
	
	select count(*) into pExists
	from i2b2metadata.table_access
	where c_name = root_node;
		
	--	add root_node if it doesn't exist
	
	if pExists = 0 then
		select tm_cz.i2b2_add_root_node(root_node,jobId) into rtnCd;
	end if;
		
	select c_hlevel into root_level
	from i2b2metadata.table_access
	where c_name = root_node;
		
	--	get study_name from new path, doesn't have to be the same as the existing study name
	
	select tm_cz.parse_nth_value(newPath, topLevel, '\') into new_study_name;
	stepCt := stepCt + 1;
	select czx_write_audit(jobId,databaseName,procedureName,'study_name: ' || new_study_name,0,stepCt,'Done') into rtnCd;
		
    --CONCEPT DIMENSION
	
	begin
	update i2b2demodata.concept_dimension
	set CONCEPT_PATH = replace(concept_path, old_path, newPath)
	where concept_path like old_path || '%';
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
	select czx_write_audit(jobId,databaseName,procedureName,'Update concept_dimension with new path',rowCt,stepCt,'Done') into rtnCd; 
    
	--I2B2
	begin
	update i2b2metadata.i2b2
	set c_fullname = replace(c_fullname, old_path, newPath)
		,c_dimcode = replace(c_fullname, old_path, newPath)
		,c_tooltip = replace(c_fullname, old_path, newPath)
		,c_hlevel =  (length(replace(c_fullname, old_path, newPath)) - nvl(length(replace(replace(c_fullname, old_path, newPath), '\')),0)) / length('\') - 2 + root_level
		,c_name = parse_nth_value(replace(c_fullname, old_path, newPath),(length(replace(c_fullname, old_path, newPath))-length(replace(replace(c_fullname, old_path, newPath),'\',null))),'\') 
	where c_fullname like old_path || '%';
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
	select czx_write_audit(jobId,databaseName,procedureName,'Update i2b2 with new path',rowCt,stepCt,'Done') into rtnCd; 
		
	--	concept_counts
		
	begin
	update i2b2demodata.concept_counts
	set concept_path = replace(concept_path, old_path, newPath)
	   ,parent_concept_path = replace(parent_concept_path, old_path, newPath)
	where concept_path like old_path || '%';
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
	select czx_write_audit(jobId,databaseName,procedureName,'Update concept_counts pass 1',rowCt,stepCt,'Done') into rtnCd; 
	
	--	update parent_concept_path for new_path (replace doesn't work)
	
	begin
	update i2b2demodata.concept_counts 
	set parent_concept_path=ltrim(SUBSTR(concept_path, 1,instr(concept_path, '\',-1,2)))
	where concept_path = newPath;
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
	select czx_write_audit(jobId,databaseName,procedureName,'Update concept_counts pass 2',rowCt,stepCt,'Done') into rtnCd; 
	
	--	fill in any upper levels
	
	select tm_cz.i2b2_fill_in_tree(null, newPath, jobID) into rtnCd;
	
	select tm_cz.i2b2_load_security_data(jobID) into rtnCd;
	
      ---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
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

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.i2b2_move_study(character varying, character varying, numeric) SET search_path=tm_cz, i2b2metadata, i2b2demodata, pg_temp;

ALTER FUNCTION tm_cz.i2b2_move_study(character varying, character varying, numeric)
  OWNER TO postgres;
