CREATE OR REPLACE function tm_cz.i2b2_load_clinical_data
(
  trial_id 			character varying
 ,top_node			character varying
 ,secure_study		character varying default 'N'
 ,highlight_study	character varying default 'N'
 ,currentJobID		numeric default -1
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
 
  topNode		varchar(2000);
  topLevel		numeric(10,0);
  root_node		varchar(2000);
  root_level	integer;
  study_name	varchar(2000);
  TrialID		varchar(100);
  secureStudy	varchar(200);
  etlDate		timestamp;
  tPath			varchar(2000);
  pCount		integer;
  pExists		integer;
  rtnCode		integer;
  tText			varchar(2000);
  v_bio_experiment_id	numeric(18,0);
  levelName		varchar(200);
  dCount		integer;
  tmp_vocab		varchar(500);
  tmp_components	varchar(1000);
  tmp_leaf		varchar(1000);
  tmp_label_vocab	varchar(500);
  tmp_label_node	varchar(2000);
  tmp_label		varchar(500);
  tmp_vocab_codes	varchar(1000);
  vCount		integer;

  regexp_date	varchar(500);
  regexp_timestamp	varchar(500);
  regexp_numeric	varchar(500);
  date_metadataxml	varchar(1000);
  num_metadataxml	varchar(1000);
  v_sqlerrm			varchar(1000);
  v_sourcesystem_ct	integer;
  v_topNode_ct		integer;

	vocab_rec	record;
	del_nodes	record;

  
	--Audit variables
	newJobFlag integer;
	databaseName VARCHAR(100);
	procedureName VARCHAR(100);
	jobID numeric(18,0);
	stepCt numeric(18,0);
	rowCt	numeric(18,0);
	rtnCd		integer;
	errorNumber		character varying;
	errorMessage	character varying;
	  
BEGIN
  
	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);

	-- regexp_date := '((((19|20)([2468][048]|[13579][26]|0[48])|2000)-02-29|((19|20)[0-9]{2}-(0[4678]|1[02])-(0[1-9]|[12][0-9]|30)|(19|20)[0-9]{2}-(0[1359]|11)-(0[1-9]|[12][0-9]|3[01])|(19|20)[0-9]{2}-02-(0[1-9]|1[0-9]|2[0-8])))\s([01][0-9]|2[0-3]):([012345][0-9]):([012345][0-9]))';
	regexp_timestamp := '(^$)|(^([1-2]\d{3}-([0]?[1-9]|1[0-2])-([0]?[1-9]|[1-2][0-9]|3[0-1]))[ |T]?((([0-1]?\d)|(2[0-3])):[0-5]\d)?(:[0-5]\d)?(\.\d{1,3})?$)';
	regexp_numeric := '^[0-9]+(\.[0-9]+)?$';
	date_metadataxml := '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>';
	num_metadataxml := '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse>LNH</Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>';
	
	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := 'TM_CZ';
	procedureName := 'I2B2_LOAD_CLINICAL_DATA';
	
	select clock_timestamp() into etlDate;

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select tm_cz.czx_start_audit (procedureName, databaseName) into jobID;
	END IF;
    	
	stepCt := 0;

	stepCt := stepCt + 1;
	tText := 'Start i2b2_load_clinical_data for ' || TrialId;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done') into rtnCd;
	
	if (secureStudy not in ('Y','N') ) then
		secureStudy := 'Y';
	end if;
	
	--	check for mismatch between TrialId and topNode for previously loaded data
	
	topNode := REGEXP_REPLACE('\' || top_node || '\','(\\){2,}', '\', 'g');
	
	select count(*) into v_sourcesystem_ct
	from i2b2metadata.i2b2
	where sourcesystem_cd = TrialId;
	
	select count(*) into v_topNode_ct
	from i2b2metadata.i2b2
	where c_fullname = topNode;
	
	if (v_sourcesystem_ct = 0 and v_topNode_ct > 0) or (v_sourcesystem_ct > 0 and v_topNode_ct = 0) then
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'TrialId and topNode are mismatched',0,stepCt,'Done') into rtnCd;	
		select  tm_cz.czx_error_handler (jobID, procedureName,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return 16;
	end if;
	
	if v_sourcesystem_ct > 0 and v_topNode_ct > 0 then
		select count(*) into v_topNode_ct
		from i2b2metadata.i2b2
		where sourcesystem_cd = TrialId
		  and c_fullname = topNode;
		if v_topNode_ct = 0 then
			stepCt := stepCt + 1;
			select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'TrialId and topNode are mismatched',0,stepCt,'Done') into rtnCd;	
			select  tm_cz.czx_error_handler (jobID, procedureName,'Application raised error') into rtnCd;
			select  tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return 16;
		end if;
	end if;
	
	--	figure out how many nodes (folders) are at study name and above
	--	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
	--	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes
	
	topLevel := length(topNode)-length(replace(topNode,'\',''));
	
	if topLevel < 3 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'Path specified in top_node must contain at least 2 nodes',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;	
	
	--	check if study data exists in lt_src_clinical_data
	
	select count(*) into pExists
	from tm_lz.lt_src_clinical_data
	where study_id = TrialId;
	
	if pExists = 0 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'No data found for study in lt_src_clinical_data',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;

	--	check if visit_date is date

	begin
	select count(*) into pExists
	from tm_lz.lt_src_clinical_data
	where visit_date is not null
	  and tm_cz.is_date(visit_date,'YYYY-MM-DD HH24:mi') = 1;
	get diagnostics rowCt := ROW_COUNT;	  
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for invalid visit_date',rowCt,stepCt,'Done') into rtnCd;
		  
	if pExists > 0 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'Invalid visit_date in tm_lz.lt_src_clinical_data',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;

	--	check if end_date is date

	begin
	select count(*) into pExists
	from tm_lz.lt_src_clinical_data
	where end_date is not null
	  and tm_cz.is_date(end_date,'YYYY-MM-DD HH24:mi') = 1;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for invalid end_date',rowCt,stepCt,'Done') into rtnCd;
		  
	if pExists > 0 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'Invalid end_date in tm_lz.lt_src_clinical_data',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;
	
	--	check if enroll_date is date

	begin
	select count(*) into pExists
	from tm_lz.lt_src_subj_enroll_date
	where enroll_date is not null
	  and tm_cz.is_date(enroll_date,'YYYY-MM-DD HH24:mi') = 1;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for invalid enroll_date',rowCt,stepCt,'Done') into rtnCd;
		  
	if pExists > 0 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'Invalid enroll_date in tm_lz.lt_src_subj_enroll_date',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;

	--	delete any existing data from lz_src_clinical_data and load new data
	
	begin
	delete from tm_lz.lz_src_clinical_data
	where study_id = TrialId;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete existing data from lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	begin
	insert into tm_lz.lz_src_clinical_data
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,etl_job_id
	,etl_date
	,data_label_ctrl_vocab_code
	,data_value_ctrl_vocab_code
	,data_label_components
	,units_cd
	,visit_date
	,link_type
	,link_value
	,end_date
	,visit_reference
	,date_ind
	,obs_string
	,valuetype_cd
	)
	select upper(study_id)
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,data_label
		  ,data_value
		  ,category_cd
		  ,jobId
		  ,etlDate
		  ,data_label_ctrl_vocab_code
		  ,data_value_ctrl_vocab_code
		  ,data_label_components
		  ,units_cd
		  ,visit_date
		  ,link_type
		  ,link_value
		  ,end_date
		  ,visit_reference
		  ,date_ind
		  ,obs_string
		  ,valuetype_cd
	from tm_lz.lt_src_clinical_data;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert data into lz_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;

	
	--	delete any existing data from lz_src_subj_enroll_date and add new
	
	begin
	delete from tm_lz.lz_src_subj_enroll_date
	where study_id = TrialId;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete existing data from lz_src_subj_enroll_date',rowCt,stepCt,'Done') into rtnCd;
	
	begin
	insert into tm_lz.lz_src_subj_enroll_date
	(study_id
	,site_id
	,subject_id
	,enroll_date
	)
	select upper(study_id)
		  ,site_id
		  ,subject_id
		  ,enroll_date
	from tm_lz.lt_src_subj_enroll_date;
	stepCt := stepCt + 1;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert data into lz_src_subj_enroll_date',rowCt,stepCt,'Done') into rtnCd;
	
	--	truncate wrk_clinical_data and load data 
	
	execute  'truncate table tm_wz.wrk_clinical_data' ;
	
	--	insert data from lt_src_clinical_data to wrk_clinical_data

	begin
	insert into tm_wz.wrk_clinical_data
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,data_label_ctrl_vocab_code
	,data_value_ctrl_vocab_code
	,data_label_components
	,units_cd
	,visit_date
	,end_date
	,data_type
	,category_path
	,usubjid
	,link_type
	,link_value
	,visit_reference
	,obs_string
	,valuetype_cd
	)
	select upper(study_id)
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,replace(data_label, '|', ',')
		  ,replace(trim('|' from data_value), '|', '-')
		  ,category_cd
		  ,data_label_ctrl_vocab_code
		  ,data_value_ctrl_vocab_code
		  ,data_label_components
		  ,units_cd
		  ,visit_date
		  ,end_date
		  ,date_ind
		  ,replace(replace(category_cd,'_',' '),'+','\')
		  ,REGEXP_REPLACE(TrialID || ':' || coalesce(site_id,'') || ':' || subject_id,'(::){1,}', ':', 'g')
		  ,link_type
		  ,link_value
		  ,trim(leading '\' from trim(trailing '\' from coalesce(visit_reference,visit_name)))
		  ,obs_string
		  ,valuetype_cd
	from tm_lz.lt_src_clinical_data
	where data_value is not null;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Load lt_src_clinical_data to work table',rowCt,stepCt,'Done') into rtnCd;  	

	-- Get study name from topNode
  
	--study_name := tm_cz.parse_nth_value(topNode, topLevel, '\');	
	study_name := replace(substr(topNode,instr(topNode,'\',-2)+1),'\','');
	
	--	Replace all underscores with spaces in topNode except those in study name

	topNode := replace(replace(topNode,'\'||study_name||'\',''),'_',' ') || '\' || study_name || '\';
	raise notice 'topNode: %', topNode;
	
	-- Get root_node from topNode
  
  	root_node := replace(substr(topNode,1,instr(topNode,'\',2)),'\','');
	--root_node := tm_cz.parse_nth_value(topNode, 2, '\');
	
	select count(*) into pExists
	from i2b2metadata.table_access
	where c_name = root_node;
	
	select count(*) into pCount
	from i2b2metadata.i2b2
	where c_name = root_node;
	
	if pExists = 0 or pCount = 0 then
		select  tm_cz.i2b2_add_root_node(root_node, jobId) into rtnCd;
	end if;
	
	select c_hlevel into root_level
	from i2b2metadata.table_access
	where c_name = root_node;
	
	--	Add any upper level nodes as needed
	
	tPath := REGEXP_REPLACE(replace(top_node,study_name,''),'(\\){2,}', '\', 'g');
	pCount := length(tPath) - length(replace(tPath,'\',''));

	if pCount > 2 then
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Adding upper-level nodes',0,stepCt,'Done') into rtnCd;
		select  tm_cz.i2b2_fill_in_tree(null, tPath, jobId) into rtnCd;
	end if;

	select count(*) into pExists
	from i2b2metadata.i2b2
	where c_fullname = topNode;
	
	--	add top node for study
	
	if pExists = 0 then
		select  tm_cz.i2b2_add_node(TrialId, topNode, study_name, jobId) into rtnCd;
	end if;

	--Remove invalid Parens in the data
	--They have appeared as empty pairs or only single ones.
  
	begin
	update tm_wz.wrk_clinical_data
	set data_value = replace(data_value,'(', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%(%' and data_value NOT like '%)%');
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 1',rowCt,stepCt,'Done') into rtnCd;
	
	begin
	update tm_wz.wrk_clinical_data
	set data_value = replace(data_value,')', '')
	where data_value like '%()%'
	   or data_value like '%( )%'
	   or (data_value like '%)%' and data_value NOT like '%(%');
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 2',rowCt,stepCt,'Done') into rtnCd;

	
	--	set data_label to null when it duplicates the last part of the category_path
	--	Remove data_label from last part of category_path when they are the same
	
	begin
	update tm_wz.wrk_clinical_data tpm
	--set data_label = null
	set category_path=substr(tpm.category_path,1,instr(tpm.category_path,'\',-2)-1)
	   ,category_cd=substr(tpm.category_cd,1,instr(tpm.category_cd,'+',-2)-1)
	where (tpm.category_cd, tpm.data_label) in
		  (select distinct t.category_cd
				 ,t.data_label
		   from tm_wz.wrk_clinical_data t
		   where upper(substr(t.category_path,instr(t.category_path,'\',-1)+1,length(t.category_path)-instr(t.category_path,'\',-1))) 
			     = upper(t.data_label)
		     and t.data_label is not null)
	  and tpm.data_label is not null;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Set data_label to null when found in category_path',rowCt,stepCt,'Done') into rtnCd;

	--	change any % to Pct and & and + to ' and ' and _ to space in data_label only
	
	begin
	update tm_wz.wrk_clinical_data
	set data_label=replace(replace(replace(replace(data_label,'%',' Pct'),'&',' and '),'+',' and '),'_',' ')
	   ,data_value=replace(replace(replace(data_value,'%',' Pct'),'&',' and '),'+',' and ')
	   ,category_cd=replace(replace(category_cd,'%',' Pct'),'&',' and ')
	   ,category_path=replace(replace(category_path,'%',' Pct'),'&',' and ');
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove percent/plus',rowCt,stepCt,'Done') into rtnCd;

  --Trim trailing and leadling spaces as well as remove any double spaces, remove space from before comma, remove trailing comma

	begin
	update tm_wz.wrk_clinical_data
	set data_label  = trim(trailing ',' from trim(replace(replace(data_label,'  ', ' '),' ,',','))),
		data_value  = trim(trailing ',' from trim(replace(replace(data_value,'  ', ' '),' ,',','))),
		visit_name  = trim(trailing ',' from trim(replace(replace(visit_name,'  ', ' '),' ,',',')));
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove leading, trailing, double spaces',rowCt,stepCt,'Done') into rtnCd;

	--	determine numeric data types, force D (dates) to be non-numeric so mixed data types gets set correctly
	--	this deals with valid date of 20130101.1230 (D) and valid number of 2013 (T) not getting tagged as numeric

	execute  'truncate table tm_wz.wt_num_data_types' ;
  
	begin
	insert into tm_wz.wt_num_data_types
	(category_cd
	,data_label
	,visit_name
	)
    select category_cd,
           data_label,
           visit_name
    from tm_wz.wrk_clinical_data
    where data_value is not null
    group by category_cd
	        ,data_label
            ,visit_name
    having sum(case when data_type = 'D' then 1 else tm_cz.is_numeric(data_value) end) = 0;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;	
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert numeric data into WZ wt_num_data_types',rowCt,stepCt,'Done') into rtnCd;
	
	--	set mixed dates to data_type = T
	
	begin
	update tm_wz.wrk_clinical_data t
	set data_type='T'
	where (coalesce(t.category_cd,'@'), coalesce(t.data_label,'@'), coalesce(t.visit_name,'@')) in
		  (select coalesce(category_cd,'@'), coalesce(data_label,'@'), coalesce(visit_name,'@')
		   from tm_wz.wrk_clinical_data
		   group by category_cd, data_label, visit_name
		   having count(distinct data_type) > 1);
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Reset mixed date/text data_types to T',rowCt,stepCt,'Done') into rtnCd;
		
	--	only update T data_types, leave D as is
	
	begin
	update tm_wz.wrk_clinical_data t
	set data_type='N'
	where exists
	     (select 1 from tm_wz.wt_num_data_types x
	      where coalesce(t.category_cd,'@') = coalesce(x.category_cd,'@')
			and coalesce(t.data_label,'@') = coalesce(x.data_label,'@')
			and coalesce(t.visit_name,'@') = coalesce(x.visit_name,'@')
		  )
	  and t.data_type = 'T';
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated data_type flag for numeric data_types',rowCt,stepCt,'Done') into rtnCd;
	
	--	set visit_date to date if D data_type and visit_date is null
	
	begin
	update tm_wz.wrk_clinical_data t
	set visit_date=to_char(to_timestamp(substr(data_value,1,8) || substr(data_value,10,4),'YYYYMMDD.HH24mi'),'YYYY-MM-DD HH24:mi')
	where t.data_type = 'D'
	  and t.visit_date is null
	   and tm_cz.is_date(substr(data_value,1,8) || substr(data_value,10,4),'YYYYMMDDHH24mi') = 0;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated visit_date for date data_types',rowCt,stepCt,'Done') into rtnCd;
	
	--	create leaf_node in wrk_clinical_data
	
	begin
	update tm_wz.wrk_clinical_data a
	set leaf_node=regexp_replace(
				case
    			--	Text data_type 
				when a.data_type = 'T'
				then case when a.category_path like '%DATALABEL%' or a.category_path like '%VISITNAME%' or a.category_path like '%OBSERVATION%'
						  then topNode || replace(replace(replace(a.category_path,'DATALABEL',coalesce(a.data_label,'')),'VISITNAME',coalesce(a.visit_name,'')),'OBSERVATION','') || '\' || a.data_value || '\'
						  else topNode || a.category_path || '\'  || coalesce(a.data_label,'') || '\' || a.data_value || '\' || coalesce(a.visit_name,'') || '\'
					 end
				--	else is numeric or date data_type and default_node
				else case when a.category_path like '%DATALABEL%' or a.category_path like '%VISITNAME%' or a.category_path like '%OBSERVATION%'
						  then topNode || replace(replace(replace(a.category_path,'DATALABEL',coalesce(a.data_label,'')),'VISITNAME',coalesce(a.visit_name,'')),'OBSERVATION',coalesce(a.obs_string,'')) || '\'
						  else topNode || a.category_path || '\'  || coalesce(a.data_label,'') || '\' || coalesce(a.visit_name,'') || '\'               
						end
				end ,'(\\){2,}', '\', 'g');
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated leaf_node in wrk_clinical_data',rowCt,stepCt,'Done') into rtnCd;	
 
	--	Check if any duplicate records of key columns (site_id, subject_id, visit_name, data_label, category_cd) for numeric data
	--	exist.  Raise error if yes
	
	execute  'truncate table tm_wz.wt_clinical_data_dups';

	begin
	insert into tm_wz.wt_clinical_data_dups
	(site_id
	,subject_id
	,visit_name
	,data_label
	,category_cd)
	select a.site_id
		  ,a.subject_id
		  ,a.visit_name
		  ,a.data_label
		  ,a.category_cd
	from tm_wz.wrk_clinical_data a
		,(select w.site_id, w.subject_id, w.leaf_node
		  from tm_wz.wrk_clinical_data w  
		  where w.data_type = 'N'
		   and w.visit_date is null
		  group by w.site_id, w.subject_id, w.leaf_node
		  having count(*) > 1) x
	where coalesce(a.site_id,'@') = coalesce(x.site_id,'@')
	  and a.subject_id = x.subject_id
	  and a.leaf_node = x.leaf_node;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for duplicate key columns',rowCt,stepCt,'Done') into rtnCd;
	if rowCt > 0 then
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Duplicate values found in key columns',rowCt,stepCt,'Done') into rtnCd;	
		select  tm_cz.czx_error_handler (jobID, procedureName,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return 16;	
	end if;

	-- Get distinct leaf node and name
 
	execute  'truncate table tm_wz.wt_trial_nodes' ;
	
	begin
	insert into tm_wz.wt_trial_nodes
	(leaf_node
	,node_name
	,data_type
	)
    select distinct leaf_node
		  ,replace(substr(leaf_node,instr(leaf_node,'\',-2)+1),'\','')
		  ,data_type
	from  tm_wz.wrk_clinical_data;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Create leaf nodes for trial',rowCt,stepCt,'Done') into rtnCd;

	--	check if any node is a parent of another, all nodes must be children
	
	begin
	select count(*) into pExists
	from tm_wz.wt_trial_nodes p
		,tm_wz.wt_trial_nodes c
	where c.leaf_node like p.leaf_node || '%' escape ''
	  and c.leaf_node != p.leaf_node;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check if node is parent of another node',rowCt,stepCt,'Done') into rtnCd;

	if pExists > 0 then
		select  tm_cz.czx_write_audit(jobId,databasename,procedurename,'Leaf node in tm_wz.wt_trial_nodes is a parent of another node',1,stepCt,'ERROR') into rtnCd;
		select  tm_cz.czx_error_handler(jobid,procedurename,'Application raised error') into rtnCd;
		select  tm_cz.czx_end_audit (jobId,'FAIL') into rtnCd;
		return 16;
	end if;
	
	--	value vocab
	
	execute  'truncate table tm_wz.wt_vocab_nodes';

	for vocab_rec in 
		select distinct leaf_node
			,coalesce(data_value_ctrl_vocab_code, data_label_ctrl_vocab_code) as data_value_ctrl_vocab_code
			,null as label_components
			,null as data_label
		from tm_wz.wrk_clinical_data
		where replace(replace(data_value_ctrl_vocab_code,';',''),'null','')	!= ''
	loop	
		dcount := length(vocab_rec.data_value_ctrl_vocab_code)-length(replace(vocab_rec.data_value_ctrl_vocab_code,';',''))+1;		
		while dcount > 0
		loop
			tmp_vocab := tm_cz.parse_nth_value(vocab_rec.data_value_ctrl_vocab_code,dcount,';');
			tmp_vocab := trim(tmp_vocab);
			begin
			insert into tm_wz.wt_vocab_nodes
			(leaf_node, modifier_cd, label_node)
			select vocab_rec.leaf_node, tmp_vocab, vocab_rec.leaf_node
			where not exists
				 (select 1 from tm_wz.wt_vocab_nodes x
				  where x.leaf_node = vocab_rec.leaf_node
				    and x.modifier_cd = tmp_vocab
					and x.label_node = vocab_rec.leaf_node);
			get diagnostics rowCt := ROW_COUNT;	  
			exception
			when others then
				errorNumber := SQLSTATE;
				errorMessage := SQLERRM;
				--Handle errors.
				select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
				--End Proc
				select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
				return -16;
			end;
			dcount := dcount - 1;
			vCount := vCount + 1;
		end loop;
	end loop;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Create modifiers for data_values',vCount,stepCt,'Done') into rtnCd;	

	--	update instance number for value modifiers
	
	begin
	update tm_wz.wt_vocab_nodes a
	set value_instance=x.instance_num
	from (select t.leaf_node
                 ,t.modifier_cd
                 ,row_number() over (partition by leaf_node order by modifier_cd) as instance_num
           from tm_wz.wt_vocab_nodes t
           where t.leaf_node = t.label_node) x
	where a.leaf_node = x.leaf_node
	  and a.modifier_cd = x.modifier_cd;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update value_instance in wt_vocab_nodes',rowCt,stepCt,'Done') into rtnCd;
					
	--	insert subjects into patient_dimension if needed
	
	execute  'truncate table tm_wz.wt_subject_info' ;

	begin
	insert into tm_wz.wt_subject_info
	(usubjid,
     age_in_years_num,
     sex_cd,
     race_cd
    )
	select a.usubjid,
	      coalesce(max(case when upper(a.data_label) = 'AGE'
					   then case when tm_cz.is_numeric(a.data_value) = 1 then 0 else a.data_value::integer end
		               when upper(a.data_label) like '%(AGE)' 
					   then case when tm_cz.is_numeric(a.data_value) = 1 then 0 else a.data_value::integer end
					   else null end),0) as age,
		  coalesce(max(case when upper(a.data_label) = 'SEX' then a.data_value
		           when upper(a.data_label) like '%(SEX)' then a.data_value
				   when upper(a.data_label) = 'GENDER' then a.data_value
				   else null end),'Unknown') as sex,
		  max(case when upper(a.data_label) = 'RACE' then a.data_value
		           when upper(a.data_label) like '%(RACE)' then a.data_value
				   else null end) as race
	from tm_wz.wrk_clinical_data a
	group by a.usubjid;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end; 
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert subject information into temp table',rowCt,stepCt,'Done') into rtnCd;
	
	--	Delete dropped subjects from patient_dimension if they do not exist in de_subject_sample_mapping
	
	begin
	delete from i2b2demodata.patient_dimension
	where sourcesystem_cd in
		 (select distinct pd.sourcesystem_cd from i2b2demodata.patient_dimension pd
		  where pd.sourcesystem_cd like TrialId || ':%' 
		  except 
		  select distinct cd.usubjid from tm_wz.wrk_clinical_data cd)
	  and patient_num not in
		  (select distinct sm.patient_id from deapp.de_subject_sample_mapping sm);
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;  
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete dropped subjects from patient_dimension',rowCt,stepCt,'Done') into rtnCd;

	--	update patients with changed information

	begin
    update i2b2demodata.patient_dimension a
	set sex_cd=x.sex_cd
	   ,race_cd=x.race_cd
	   ,age_in_years_num=x.age_in_years_num
	   ,update_date=etlDate
	from (select t.usubjid as sourcesystem_cd
                 ,t.sex_cd
                 ,t.race_cd
                 ,t.age_in_years_num
           from tm_wz.wt_subject_info t
			   ,i2b2demodata.patient_dimension pd
           where pd.sourcesystem_cd = t.usubjid
			and t.sex_cd != pd.sex_cd
			and t.age_in_years_num != pd.age_in_years_num
			and t.race_cd != pd.race_cd) x
	where a.sourcesystem_cd = x.sourcesystem_cd;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update subjects with changed demographics in patient_dimension',rowCt,stepCt,'Done') into rtnCd;

	--	insert new subjects into patient_dimension
	
	begin
	insert into i2b2demodata.patient_dimension
    (patient_num,
     sex_cd,
     age_in_years_num,
     race_cd,
     update_date,
     download_date,
     import_date,
     sourcesystem_cd
    )
    select nextval('i2b2demodata.sq_patient_num'),
		   t.sex_cd,
		   t.age_in_years_num,
		   t.race_cd,
		   etlDate,
		   etlDate,
		   etlDate,
		   t.usubjid
    from tm_wz.wt_subject_info t
	where t.usubjid in 
		 (select distinct cd.usubjid from tm_wz.wt_subject_info cd
		  except
		  select distinct pd.sourcesystem_cd from i2b2demodata.patient_dimension pd
		  where pd.sourcesystem_cd like TrialId || ':%');
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;	  
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert new subjects into patient_dimension',rowCt,stepCt,'Done') into rtnCd;
	
	--	new bulk delete of unused nodes
	
	execute  'truncate table tm_wz.wt_del_nodes';
	stepCt := stepCt + 1;	
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Truncate table tm_wz.wt_del_nodes',0,stepCt,'Done') into rtnCd;
	
	begin
	insert into tm_wz.wt_del_nodes
	select l.c_fullname
		  ,l.c_basecode
	from i2b2metadata.i2b2 l
	where l.c_visualattributes like 'L%'
	  and l.c_fullname like topNode || '%' escape ''
	  and l.c_fullname not in
		 (select t.leaf_node 
		  from tm_wz.wt_trial_nodes t
		  union
		  select distinct p.c_fullname as leaf_node
		  from deapp.de_subject_sample_mapping sm
			  ,i2b2metadata.i2b2 c
			  ,i2b2metadata.i2b2 p
		  where sm.trial_name = TrialId
		    and sm.concept_code = c.c_basecode
		    and c.c_fullname like p.c_fullname || '%' escape ''
			and p.c_fullname > topNode
			);
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;	
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert nodes into tm_wz.wt_del_nodes',rowCt,stepCt,'Done') into rtnCd;
	
	if rowCt > 0 then
	
		--	delete i2b2 unused nodes
		begin
		delete from i2b2metadata.i2b2 f
		where f.c_fullname in (select distinct x.c_fullname from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from i2b2',rowCt,stepCt,'Done') into rtnCd;
		
		--	delete concept_dimension unused nodes
		
		begin
		delete from i2b2demodata.concept_dimension f
		where f.concept_cd in (select distinct x.c_basecode as concept_cd from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from concept_dimension',rowCt,stepCt,'Done') into rtnCd;
		
		--	delete observation_fact unused nodes
		
		begin
		delete from i2b2demodata.observation_fact f
		where f.concept_cd in (select distinct x.c_basecode as concept_cd from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from observation_fact',rowCt,stepCt,'Done') into rtnCd;
		
		--	delete de_concept_visit unused nodes
		
		begin
		delete from deapp.de_concept_visit f
		where f.concept_cd in (select distinct x.c_basecode as concept_cd from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from de_concept_visit',rowCt,stepCt,'Done') into rtnCd;
		
	end if;

	--	bulk insert leaf nodes

	begin
	update i2b2demodata.concept_dimension a
	set name_char=x.node_name
	from (select t.leaf_node as concept_path
                 ,t.node_name
           from tm_wz.wt_trial_nodes t
			   ,i2b2demodata.concept_dimension c
           where t.leaf_node = c.concept_path
              and t.node_name != c.name_char) x
	where a.concept_path = x.concept_path;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update name_char in concept_dimension for changed names',rowCt,stepCt,'Done') into rtnCd;
	
	begin
	insert into i2b2demodata.concept_dimension
    (concept_cd
	,concept_path
	,name_char
	,update_date
	,download_date
	,import_date
	,sourcesystem_cd
	)
    select nextval('i2b2demodata.concept_id')::text
	     ,x.leaf_node
		 ,x.node_name
		 ,etlDate
		 ,etlDate
		 ,etlDate
		 ,TrialId
	from (select distinct c.leaf_node
				,c.node_name
		  from tm_wz.wt_trial_nodes c
		  where not exists
			(select 1 from i2b2demodata.concept_dimension x
			where c.leaf_node = x.concept_path)
		 ) x;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted new leaf nodes into I2B2DEMODATA concept_dimension',rowCt,stepCt,'Done') into rtnCd;

	--	convert D data_type to T for i2b2
	
	begin
	update i2b2metadata.i2b2 a
	set c_name=upd.node_name
	   ,c_visualattributes=case when upd.data_type = 'D' then 'LAD' else 'LA' end
	   ,c_metadataxml=case when upd.data_type = 'T'
								then null
							when upd.data_type = 'D' 
								then date_metadataxml
							else num_metadataxml end
	from (select distinct t.leaf_node as c_fullname
                 ,t.data_type  --t.data_type
                 ,t.node_name
           from tm_wz.wt_trial_nodes t
              ,i2b2metadata.i2b2 c
           where t.leaf_node = c.c_fullname 
             and (t.node_name != c.c_name or
			       c.c_columndatatype != t.data_type or
				  (c.c_metadataxml is not null and t.data_type = 'T'))) upd
    where a.c_fullname = upd.c_fullname;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated name and data type in i2b2 if changed',rowCt,stepCt,'Done') into rtnCd;
			   
	begin
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
	,m_applied_path
	,c_metadataxml
	,i2b2_id
	)
    select x.c_hlevel
		  ,x.concept_path
		  ,x.name_char
		  ,case when x.data_type = 'D' then 'LAD' else 'LA' end
		  ,'N'
		  ,'CONCEPT_CD'
		  ,'CONCEPT_DIMENSION'
		  ,'CONCEPT_PATH'
		  ,x.concept_path
		  ,x.concept_path
		  ,etlDate
		  ,etlDate
		  ,etlDate
		  ,TrialId
		  ,x.concept_cd
		  ,'LIKE'
		  ,'T'
		  ,'trial:' || TrialId
		  ,'@'
		  ,x.c_metadataxml
		  ,nextval('i2b2metadata.sq_i2b2_id')
	from (select distinct (length(c.concept_path) - coalesce(length(replace(c.concept_path, '\','')),0)) / length('\') - 2 + root_level as c_hlevel
		  ,c.concept_path
		  ,c.concept_cd
		  ,c.name_char
		  ,case when t.data_type = 'T' then null
		        when t.data_type = 'D' then date_metadataxml
		   else num_metadataxml
		   end as c_metadataxml
		  ,t.data_type
		 from i2b2demodata.concept_dimension c
			 ,tm_wz.wt_trial_nodes t
		 where c.concept_path = t.leaf_node
		  and not exists
			 (select 1 from i2b2metadata.i2b2 x
			  where c.concept_path = x.c_fullname)
		) x;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;  
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted leaf nodes into I2B2METADATA i2b2',rowCt,stepCt,'Done') into rtnCd;

	--	delete all facts for clinical data
	
	begin
	delete from i2b2demodata.observation_fact f
	where (f.modifier_cd = TrialId or f.sourcesystem_cd = TrialId)
	  and f.concept_cd not in
		 (select distinct concept_code as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and concept_code is not null
		  union
		  select distinct concept_cd as concept_cd from deapp.de_subject_snp_dataset
		  where trial_name = TrialId
		    and concept_cd is not null);
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete clinical data for study from observation_fact',rowCt,stepCt,'Done') into rtnCd;

	--	create encounter_num for each link type/value 

	begin
	delete from deapp.de_encounter_type
	where study_id = TrialId;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete study from deapp.de_encounter_type',rowCt,stepCt,'Done') into rtnCd;	
	
	begin
	insert into deapp.de_encounter_type
	select TrialId
		  ,x.link_type
		  ,x.link_value
		  ,nextval('i2b2demodata.seq_encounter_num')
	from (select distinct link_type, link_value from tm_wz.wrk_clinical_data) x;
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Add study to deapp.de_encounter_type',rowCt,stepCt,'Done') into rtnCd;	

	begin
	insert into i2b2demodata.observation_fact
	(encounter_num
	,patient_num
    ,concept_cd
    ,modifier_cd
    ,valtype_cd
    ,tval_char
    ,nval_num
    ,sourcesystem_cd
    ,import_date
    ,valueflag_cd
    ,provider_id
    ,location_cd
	,units_cd
	,instance_num
	,start_date
	,end_date
	)
	select distinct enc.encounter_num
		  ,c.patient_num
		  ,i.c_basecode
		  ,coalesce(vv.modifier_cd,'@') as modifier_cd
		  ,a.data_type
		  ,case when a.data_type = 'T' then a.data_value
				else 'E'  --Stands for Equals for numeric types
				end as tval_char
		  ,case when a.data_type != 'T' then a.data_value
				else null --Null for text types
				end as nval_num
		  ,a.study_id
		  ,etlDate
		  ,case when a.valuetype_cd is null then '@' else coalesce(a.valuetype_cd,'N') end as valuetype_cd
		  ,'@'
		  ,'@'
		  ,units_cd
		--  ,1
		 ,coalesce(vv.value_instance,1)
		 --,row_number() over (partition by enc.encounter_num, i.c_basecode order by coalesce(vv.modifier_cd,'@')) as instance_num
		 ,case when a.visit_date is null then null else to_timestamp(a.visit_date,'YYYY-MM-DD HH24:mi') end
		 ,case when a.end_date is null then null else to_timestamp(a.end_date,'YYYY-MM-DD HH24:mi') end
	from tm_wz.wrk_clinical_data a
		 inner join deapp.de_encounter_type enc
			   on  a.link_type = enc.link_type
			   and a.link_value = enc.link_value
			   and enc.study_id = TrialId
		 inner join i2b2demodata.patient_dimension c
             on  a.usubjid = c.sourcesystem_cd
		 inner join i2b2metadata.i2b2 i
             on a.leaf_node = i.c_fullname
		 left outer join tm_wz.wt_vocab_nodes vv
			 on a.leaf_node = vv.label_node;  
	get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial into I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;	
			
	-- fill in folder nodes 
  
	select  tm_cz.i2b2_fill_in_tree(TrialId, topNode, jobID) into rtnCd;
		
	--	insert study-level modifer and modifier for each patient at study-level
	
	select count(*) into pExists
	from tm_wz.wt_vocab_nodes;
		
	if pExists > 0 then
		select count(*) into pCount
		from i2b2demodata.modifier_dimension
		where modifier_path = '\' || 'Study' || '\';
			
		--	insert top_level \Study\ modifier if not found

		if pCount = 0 then 
			begin
			insert into i2b2demodata.modifier_dimension
			(modifier_path
			,modifier_cd
			,name_char
			,modifier_level
			,modifier_node_type
			)
			select '\' || 'Study' || '\'
			,'CSTUDY' 
			,'Study'
			,0
			,'F';
			get diagnostics rowCt := ROW_COUNT;	  
			exception
			when others then
				errorNumber := SQLSTATE;
				errorMessage := SQLERRM;
				--Handle errors.
				select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
				--End Proc
				select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
				return -16;
			end;
			stepCt := stepCt + 1;
			select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert Study modifier into modifier_dimension',rowCt,stepCt,'Done') into rtnCd;
		end if;
		
		select count(*) into pCount
		from i2b2demodata.modifier_metadata
		where modifier_cd = 'CSTUDY';
		
		--	insert top_level \Study\ metadata if not found

		if pCount = 0 then 
			begin
			insert into i2b2demodata.modifier_metadata
			(modifier_cd
			,valtype_cd
			,std_units
			,visit_ind
			)
			select 'CSTUDY' 
			,'T'
			,null
			,'N';
			get diagnostics rowCt := ROW_COUNT;	  
			exception
			when others then
				errorNumber := SQLSTATE;
				errorMessage := SQLERRM;
				--Handle errors.
				select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
				--End Proc
				select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
				return -16;
			end;
			stepCt := stepCt + 1;
			select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert \Study\ modifier metadata into modifier_metadata',rowCt,stepCt,'Done') into rtnCd;				
		end if;

		--	insert study into modifier_dimension
			
		begin
		delete from i2b2demodata.modifier_dimension
		where modifier_cd = 'STUDY:' || TrialId;
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
				select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete study record from modifier_dimension',rowCt,stepCt,'Done') into rtnCd;				

		begin
		insert into i2b2demodata.modifier_dimension
		(modifier_path
		,modifier_cd
		,name_char
		,modifier_level
		,modifier_node_type
		,sourcesystem_cd
		)
		select '\' || 'Study' || '\' || i.c_name || '\'
		,'STUDY:' || TrialId
		,i.c_name
		,1
		,'L'
		,TrialId
		from i2b2metadata.i2b2 i
		where i.sourcesystem_cd = TrialId
		  and i.c_hlevel = (select min(x.c_hlevel) from i2b2metadata.i2b2 x
							where x.sourcesystem_cd = TrialId);
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert modifier for study into modifier_dimension',rowCt,stepCt,'Done') into rtnCd;	
			
		-- insert study into modifier_metadata
			
		select count(*) into pCount
		from i2b2demodata.modifier_metadata
		where modifier_cd = 'STUDY:' || TrialId;

		if pCount = 0 then	
			begin
			insert into i2b2demodata.modifier_metadata
			(modifier_cd
			,valtype_cd
			,std_units
			,visit_ind)
			select 'STUDY:' || TrialId
			,'T'
			,null
			,'N';
			get diagnostics rowCt := ROW_COUNT;	  
			exception
			when others then
				errorNumber := SQLSTATE;
				errorMessage := SQLERRM;
				--Handle errors.
				select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
				--End Proc
				select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
				return -16;
			end;
			stepCt := stepCt + 1;
			select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert modifier for study into modifier_metadata',rowCt,stepCt,'Done') into rtnCd;	
		end if;
		
		--	insert observation_fact for each patient at study-level
		
		begin
		insert into i2b2demodata.observation_fact
		(encounter_num,
		 patient_num,
		 concept_cd,
		 modifier_cd,
		 valtype_cd,
		 tval_char,
		 nval_num,
		 sourcesystem_cd,
		 import_date,
		 valueflag_cd,
		 provider_id,
		 location_cd,
		 instance_num
		)
		select c.patient_num*-1,
			   c.patient_num,
			   (select i.c_basecode from i2b2metadata.i2b2 i
				where i.sourcesystem_cd = TrialId
				  and i.c_hlevel = (select min(x.c_hlevel) from i2b2metadata.i2b2 x
									where x.sourcesystem_cd = TrialId)) as concept_cd,
			   'STUDY:' || TrialId,
			   'T',
			   'E',
			   null,
			   TrialId, 
			   etlDate, 
			   '@',
			   '@',
			   '@',
				1
		from i2b2demodata.patient_dimension c
		where c.sourcesystem_cd like TrialId || ':%';
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert study-level modifiers into observation_fact',rowCt,stepCt,'Done') into rtnCd;		
	else
		--	no modifiers for study, remove study-level modifier from modifier_dimension and modifier_metadata
		
		begin
		delete from i2b2demodata.modifier_dimension
		where modifier_cd = 'STUDY:' || TrialId;
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete study-level modifiers from modifier_dimension',rowCt,stepCt,'Done') into rtnCd;		
		
		begin
		delete from i2b2demodata.modifier_metadata
		where modifier_cd = 'STUDY:' || TrialId;
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete study-level modifiers from modifier_metadata',rowCt,stepCt,'Done') into rtnCd;	
		
	end if;

	--	insert records to de_concept_visit if not exists
	
	begin
	insert into deapp.de_concept_visit
	(concept_cd
	,visit_name
	,sourcesystem_cd
	)
	select distinct cd.concept_cd as concept_cd
		  ,t.visit_name
		  ,TrialId as sourcesystem_cd
	from tm_wz.wrk_clinical_data t
		,i2b2demodata.concept_dimension cd
	where t.leaf_node = cd.concept_path
	  and t.data_type != 'D'
	  and t.visit_name is not null
	  and not exists
		  (select 1 from deapp.de_concept_visit x
		   where cd.concept_cd = x.concept_cd
		     and t.visit_name = x.visit_name
			 and x.sourcesystem_cd = TrialId);
	get diagnostics rowCt := ROW_COUNT;	  
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert data into de_concept_visit',rowCt,stepCt,'Done') into rtnCd;	
	
	--	populate deapp.de_encounter_level with highest level of same encounter type
	
	begin
	delete from deapp.de_encounter_level
	where study_id = TrialId;
	get diagnostics rowCt := ROW_COUNT;	  
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete study from deapp.de_encounter_level',rowCt,stepCt,'Done') into rtnCd;	
	
	begin
	insert into deapp.de_encounter_level 
	(study_id
	,concept_cd
	,link_type
	)
	select TrialId
		  ,x.c_basecode
		  ,case when x.subject_type = 1 and x.enc_type = 0 then 'S'
            when x.subject_type = 0 and x.enc_type = 1 then 'E'
            else 'M' end as enc_type
	from (select p.c_basecode
				,max(case when t.link_type = 'SUBJECT' then 1 else 0 end) as subject_type
				,max(case when t.link_type = 'SUBJECT' then 0 else 1 end) as enc_type
		 from tm_wz.wrk_clinical_data t
			 ,i2b2metadata.i2b2 p
			 ,i2b2metadata.i2b2 c
		 where t.leaf_node = c.c_fullname
		   and c.c_fullname like p.c_fullname || '%' escape ''
		   and p.sourcesystem_cd = TrialId
		 group by p.c_basecode) x;
	get diagnostics rowCt := ROW_COUNT;	  
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert study into deapp.de_encounter_level',rowCt,stepCt,'Done') into rtnCd;	
	
	select count(*) into pExists
	from tm_lz.lt_src_subj_enroll_date
	where study_id = TrialId;
	
	if pExists > 0 then 
	
		--	calculate days_since_enroll
		
		begin
		delete from deapp.de_obs_enroll_days
		where study_id = TrialId;
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete existing data from deapp.de_obs_enroll_days',rowCt,stepCt,'Done') into rtnCd;	
		
		begin
		insert into deapp.de_obs_enroll_days
		(encounter_num
		,days_since_enroll
		,study_id
		,visit_date)
		select distinct enc.encounter_num
			  ,round((minutes_between(enc.start_date,to_timestamp(enr.enroll_date,'YYYY-MM-DD'))::numeric)/(24*60),5)
			  ,TrialId
			  ,enc.start_date
		from i2b2demodata.observation_fact enc
		inner join i2b2demodata.patient_dimension pd
			  on  enc.patient_num = pd.patient_num
		left outer join tm_lz.lt_src_subj_enroll_date enr
			  on REGEXP_REPLACE(TrialID || ':' || coalesce(enr.site_id,'') || ':' || enr.subject_id,'(::){1,}', ':', 'g') = pd.sourcesystem_cd
		where enc.sourcesystem_cd = TrialId
		  and enc.start_date is not null
		  and enc.encounter_num is not null
		  and enr.ENROLL_DATE is not null
		  and enc.concept_cd != 'SECURITY';
		get diagnostics rowCt := ROW_COUNT;	  
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert data in deapp.de_obs_enroll_days',rowCt,stepCt,'Done') into rtnCd;	
	end if;
	
	--	update c_visualattributes for all nodes in study, done to pick up node that changed from leaf/numeric to folder/text
	
	begin
	update i2b2metadata.i2b2 a
	set c_visualattributes=case when upd.nbr_children = 1 
								then 'L' || substr(a.c_visualattributes,2,2)
								else 'F' || substr(a.c_visualattributes,2,1) ||
								case when upd.c_fullname = topNode -- and highlight_study = 'Y'
									then 'S' else substr(a.c_visualattributes,3,1) end
						   end
	   ,c_metadataxml=case when upd.nbr_children = 1 then a.c_metadataxml else null end
	from (select p.c_fullname, count(*) as nbr_children 
				 from i2b2metadata.i2b2 p
					 ,i2b2metadata.i2b2 c
				 where p.c_fullname like topNode || '%' escape ''
				   and c.c_fullname like p.c_fullname || '%' escape ''
				 group by p.c_fullname) upd
	where a.c_fullname = upd.c_fullname;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update c_visualattributes for study',rowCt,stepCt,'Done') into rtnCd;
    
	--	set sourcesystem_cd, c_comment to null if any added upper-level nodes
		
	begin
	update i2b2metadata.i2b2 b
	set sourcesystem_cd=null,c_comment=null
	where b.sourcesystem_cd = TrialId
	  and length(b.c_fullname) < length(topNode);
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Set sourcesystem_cd to null for added upper-level nodes',rowCt,stepCt,'Done') into rtnCd;
	
	select  tm_cz.i2b2_create_concept_counts(topNode, jobID) into rtnCd;
	
		--	new bulk delete of unused nodes
	
	execute  'truncate table tm_wz.wt_del_nodes' ;
	stepCt := stepCt + 1;	
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Truncate table tm_wz.wt_del_nodes for hidden nodes',0,stepCt,'Done') into rtnCd;
	
	begin
	insert into tm_wz.wt_del_nodes
	select l.c_fullname
		  ,l.c_basecode
	from i2b2metadata.i2b2 l
	where substr(l.c_visualattributes,2,1) = 'H'
	  and l.c_fullname like topNode || '%' escape '';
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;	
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert hidden nodes into tm_wz.wt_del_nodes',rowCt,stepCt,'Done') into rtnCd;
	
	select count(*) into pExists
	from tm_wz.wt_del_nodes;
	
	if pExists > 0 then 
	
		--	delete i2b2 unused nodes
		
		begin
		delete from i2b2metadata.i2b2 f
		where f.c_fullname in (select distinct x.c_fullname from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete hidden nodes from i2b2',rowCt,stepCt,'Done') into rtnCd;
		
		--	delete concept_dimension unused nodes
		
		begin
		delete from i2b2demodata.concept_dimension f
		where f.concept_cd in (select distinct x.c_basecode as concept_cd from tm_wz.wt_del_nodes x);
		get diagnostics rowCt := ROW_COUNT;
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;	
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Bulk delete hidden nodes from concept_dimension',rowCt,stepCt,'Done') into rtnCd;
		
	end if;

/*	
	--	create entries to support FMAPP
	
	select count(*) into pExists
	from biomart.bio_experiment
	where accession = TrialId;
	
	if pExists = 0 then
		--	insert placeholder for study in bio_experiment
		insert into biomart.bio_experiment
		(title, accession, etl_id)
		select 'Metadata not available'
			  ,TrialId
			  ,'METADATA:' || TrialId;
		rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial/study into biomart.bio_experiment',rowCt,stepCt,'Done') into rtnCd;
	end if;
	
	select bio_experiment_id into v_bio_experiment_id
	from biomart.bio_experiment
	where accession = TrialId;
	
	--	insert study into biomart.bio_data_uid
	
	select count(*) into pExists
	from biomart.bio_data_uid
	where bio_data_id = v_bio_experiment_id;
	
	if pexists = 0 then
		insert into biomart.bio_data_uid
		(bio_data_id
		,unique_id 
		,bio_data_type
		)
		select v_bio_experiment_id
			  ,'EXP:' || TrialId
			  ,'Experiment';
		rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial/study into biomart.bio_data_uid',rowCt,stepCt,'Done') into rtnCd;
	end if;
	
	select count(*) into pExists
	from fmapp.fm_folder
	where folder_name = TrialId;
	
	if pExists = 0 then
		--	insert study into fmapp.fm_folder
		insert into fmapp.fm_folder
		(folder_name 
		,folder_level      
		,folder_type
		,active_ind
		)
		select TrialId
			  ,1
			  ,'STUDY'
			  ,'1';
		rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial/study into fmapp.fm_folder',rowCt,stepCt,'Done') into rtnCd;
	end if;
	
	--	insert study into folder association
	
	select count(*) into pExists
	from fmapp.fm_folder_association
	where object_uid = 'EXP:' || TrialId;
	
	if pExists = 0 then 
		insert into fmapp.fm_folder_association
		(folder_id
		,object_uid
		,object_type
		)
		select ff.folder_id
			  ,'EXP:' || TrialId
			  ,'bio.Experiment'
		from fmapp.fm_folder ff
		where folder_name = TrialId;
		rowCt := ROW_COUNT;
		stepCt := stepCt + 1;
		select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial/study into fmapp.fm_folder_asociation',rowCt,stepCt,'Done') into rtnCd;
	end if;  
*/

	select  tm_cz.i2b2_create_security_for_trial(TrialId, secureStudy, jobID) into rtnCd;
	select  tm_cz.i2b2_load_security_data(jobID) into rtnCd;
	
	stepCt := stepCt + 1;
	select  tm_cz.czx_write_audit(jobId,databaseName,procedureName,'End i2b2_load_clinical_data',0,stepCt,'Done') into rtnCd;
	
    ---Cleanup OVERALL JOB if this proc is being run standalone
	if newJobFlag = 1
	then
		select  tm_cz.czx_end_audit (jobID, 'SUCCESS') into rtnCd;
	end if;

	return 0;
  
/*
	exception	
	when others then
	v_sqlerrm := substr(SQLERRM,1,1000);
		raise notice 'error: %', v_sqlerrm;
		--Handle errors.
		select  tm_cz.czx_error_handler (jobID, procedureName,v_sqlerrm);
		--End Proc
		select  tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return 16;
*/
	
END;

$$ LANGUAGE plpgsql
security definer 
-- set a secure search_path: trusted schema(s), then pg_temp
set search_path=tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp;

