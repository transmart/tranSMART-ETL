-- Function: tm_cz.i2b2_load_clinical_data(character varying, character varying, character varying, character varying, numeric)

-- DROP FUNCTION tm_cz.i2b2_load_clinical_data(character varying, character varying, character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION tm_cz.i2b2_load_clinical_data(trial_id character varying, top_node character varying, secure_study character varying DEFAULT 'N'::character varying, highlight_study character varying DEFAULT 'N'::character varying, currentjobid numeric DEFAULT (-1))
  RETURNS numeric AS
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
 
	--Audit variables
	newJobFlag		integer;
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			integer;
	
	topNode			varchar(2000);
	topLevel		numeric(10,0);
	root_node		varchar(2000);
	root_level		integer;
	study_name		varchar(2000);
	TrialID			varchar(100);
	secureStudy		varchar(200);
	etlDate			timestamp;
	tPath			varchar(2000);
	pCount			integer;
	pExists			integer;
	rtnCode			integer;
	tText			varchar(2000);
	v_bio_experiment_id	numeric(18,0);
	levelName		varchar(200);
	dCount			integer;
	vCount			integer;
	tmp_vocab		varchar(500);
	tmp_components	varchar(1000);
	tmp_leaf		varchar(1000);
	tmp_label_vocab	varchar(500);
	tmp_label		varchar(500);
	tmp_vocab_codes	varchar(1000);	
	
	v_sourcesystem_ct	int;
	v_topNode_ct		int;
  
	addNodes CURSOR is
	select DISTINCT leaf_node, node_name
	from  tm_wz.wt_trial_nodes a;
   
	--	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

	delNodes CURSOR is
	select distinct c_fullname 
	from  i2b2metadata.i2b2
	where c_fullname like topNode || '%' escape '`'
      and substr(c_visualattributes,2,1) = 'H';
	  
	--	cursor to determine if any leaf nodes exist in i2b2 that are not used in this reload (node changes from text to numeric or numeric to text)
	  
	delUnusedLeaf cursor is
	select l.c_fullname
	from i2b2metadata.i2b2 l
	where l.c_visualattributes like 'L%'
	  and l.c_fullname like topNode || '%' escape '`'
	  and l.c_fullname not in
		 (select t.leaf_node 
		  from tm_wz.wt_trial_nodes t
		  union
		  select m.c_fullname
		  from deapp.de_subject_sample_mapping sm
			  ,i2b2metadata.i2b2 m
		  where sm.trial_name = TrialId
		    and sm.concept_code = m.c_basecode
			and m.c_visualattributes like 'L%');

BEGIN
  
	TrialID := upper(trial_id);
	secureStudy := upper(secure_study);
	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;
	select clock_timestamp() into etlDate;

	databaseName := 'tm_cz';
	procedureName := 'i2b2_load_clinical_data';

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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done') into rtnCd;
	
	if (secureStudy not in ('Y','N') ) then
		secureStudy := 'Y';
	end if;
	
	topNode := REGEXP_REPLACE('\' || top_node || '\','(\\){2,}', '\', 'g');
	
	--	check for mismatch between TrialId and topNode for previously loaded data
	
	select count(*) into v_sourcesystem_ct
	from i2b2metadata.i2b2
	where sourcesystem_cd = TrialId;
	
	select count(*) into v_topNode_ct
	from i2b2metadata.i2b2
	where c_fullname = topNode;
	
	if (v_sourcesystem_ct = 0 and v_topNode_ct > 0) or (v_sourcesystem_ct > 0 and v_topNode_ct = 0) then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'TrialId and topNode are mismatched',0,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;
	
	if v_sourcesystem_ct > 0 and v_topNode_ct > 0 then
		select count(*) into v_topNode_ct
		from i2b2metadata.i2b2
		where sourcesystem_cd = TrialId
		  and c_fullname = topNode;
		if v_topNode_ct = 0 then
			stepCt := stepCt + 1;
			select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'TrialId and topNode are mismatched',0,stepCt,'Done') into rtnCd;	
			select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end if;
	end if;
	
	--	figure out how many nodes (folders) are at study name and above
	--	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
	--	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes
	
	select length(topNode)-length(replace(topNode,'\','')) into topLevel;
	
	if topLevel < 3 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Path specified in top_node must contain at least 2 nodes',0,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;	
	end if;	
		
	--	truncate tm_wz.wrk_clinical_data and load data from tm_lz.lt_src_clinical_data
	
	execute ('truncate table tm_wz.wrk_clinical_data');
	
	--	insert data from lt_src_clinical_data to tm_wz.wrk_clinical_data
	
	begin
	insert into tm_wz.wrk_clinical_data
	(study_id
	,site_id
	,subject_id
	,visit_name
	,data_label
	,data_value
	,category_cd
	,category_path
	,usubjid
	,units_cd
	,visit_date
	,end_date
	,obs_string
	,date_ind
	,data_type
	,valuetype_cd
	)
	select study_id
		  ,site_id
		  ,subject_id
		  ,visit_name
		  ,replace(data_label, '|', ',')
		  ,replace(trim('|' from data_value),'|','-')
		  ,category_cd
		  ,replace(replace(category_cd,'_',' '),'+','\')
		  ,REGEXP_REPLACE(TrialID || ':' || coalesce(site_id,'') || ':' || subject_id,'(::){1,}', ':','g')
		  ,units_cd
		  ,visit_date
		  ,end_date
		  ,obs_string
		  ,date_ind
		  ,case when date_ind = 'D' then 'D' else 'T' end as data_type
		  ,valuetype_cd
	from tm_lz.lt_src_clinical_data;
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
	get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Load lt_src_clinical_data to work table',rowCt,stepCt,'Done') into rtnCd;

	-- Get study name from topNode
  
	select tm_cz.parse_nth_value(topNode, topLevel, '\') into study_name;	
	
	--	Replace all underscores with spaces in topNode except those in study name
	topNode := replace(replace(topNode,'\'||study_name||'\',''),'_',' ') || '\' || study_name || '\';

	-- Get root_node from topNode
  
	select tm_cz.parse_nth_value(topNode, 2, '\') into root_node;
	
	select count(*) into pExists
	from i2b2metadata.table_access
	where c_name = root_node;
	
	select count(*) into pCount
	from i2b2metadata.i2b2
	where c_name = root_node;
	
	if pExists = 0 or pCount = 0 then
		select tm_cz.i2b2_add_root_node(root_node, jobId) into rtnCd;
		if rtnCd > 0 then
			stepCt := stepCt + 1;
			select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Unable to add root_node: '||root_node,0,stepCt,'Done') into rtnCd;	
			select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end if;
	end if;
	
	select c_hlevel into root_level
	from i2b2metadata.table_access
	where c_name = root_node;
	
	--	Add any upper level nodes as needed
	
	tPath := REGEXP_REPLACE(replace(top_node,study_name,''),'(\\){2,}', '\', 'g');
	select length(tPath) - length(replace(tPath,'\','')) into pCount;

	if pCount > 2 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Adding upper-level nodes',0,stepCt,'Done') into rtnCd;
		select tm_cz.i2b2_fill_in_tree(null, tPath, jobId) into rtnCd;
	end if;

	select count(*) into pExists
	from i2b2metadata.i2b2
	where c_fullname = topNode;
	
	--	add top node for study
	
	if pExists = 0 then
		select tm_cz.i2b2_add_node(TrialId, topNode, study_name, jobId) into rtnCd;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 1',rowCt,stepCt,'Done') into rtnCd;
	
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove empty parentheses 2',rowCt,stepCt,'Done') into rtnCd;

	--	set data_label to null when it duplicates the last part of the category_path
	--	Remove data_label from last part of category_path when they are the same

	begin
	update tm_wz.wrk_clinical_data tpm
	--set data_label = null
	set category_path=substr(tpm.category_path,1,tm_cz.instr(tpm.category_path,'\',-2,1)-1)
	   ,category_cd=substr(tpm.category_cd,1,tm_cz.instr(tpm.category_cd,'+',-2,1)-1)
	where (tpm.category_cd, tpm.data_label) in
		  (select distinct t.category_cd
				 ,t.data_label
		   from tm_wz.wrk_clinical_data t
		   where upper(substr(t.category_path,tm_cz.instr(t.category_path,'\',-1,1)+1,length(t.category_path)-tm_cz.instr(t.category_path,'\',-1,1))) 
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Set data_label to null when found in category_path',rowCt,stepCt,'Done') into rtnCd;

	--	change any % to Pct and & and + to ' and ' and _ to space in data_label only
	
	begin
	update tm_wz.wrk_clinical_data
	set data_label=replace(replace(replace(replace(data_label,'%',' Pct'),'&',' and '),'+',' and '),'_',' ')
	   ,data_value=replace(replace(replace(data_value,'%',' Pct'),'&',' and '),'+',' and ')
	   ,category_cd=replace(replace(category_cd,'%',' Pct'),'&',' and ')
	   ,category_path=replace(replace(category_path,'%',' Pct'),'&',' and ');
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Remove leading, trailing, double spaces',rowCt,stepCt,'Done') into rtnCd;
	
	--	check if visit_date is date

	select count(*) into rowCt
	from tm_wz.wrk_clinical_data
	where visit_date is not null
	  and tm_cz.is_date(visit_date,'YYYY-MM-DD HH24:mi') = 1;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for invalid visit_date',rowCt,stepCt,'Done') into rtnCd;
		  
	if rowCt > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Invalid visit_date in tm_lz.lt_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;	
	end if;	

	--	check if end_date is date

	select count(*) into rowCt
	from tm_wz.wrk_clinical_data
	where end_date is not null
	  and tm_cz.is_date(end_date,'YYYY-MM-DD HH24:mi') = 1;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for invalid end_date',rowCt,stepCt,'Done') into rtnCd;
		  
	if rowCt > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Invalid end_date in tm_lz.lt_src_clinical_data',rowCt,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;	
	end if;	

/*	
	--	check if enroll_date is date
	
	select count(*) into pExists
	from tm_lz.lt_src_subj_enroll_date
	where enroll_date is not null
	  and is_date(enroll_date,'YYYY/MM/DD HH24:mi') = 1;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Check for invalid enroll_date',rowCt,stepCt,'Done');
		  
	if pExists > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Invalid enroll_date in tm_lz.lt_src_subj_enroll_date',pExists,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end if;
*/
	
	--	determine numeric data types, force D (dates) to be non-numeric so mixed data types gets set correctly
	--	this deals with valid date of 20130101.1230 (D) and valid number of 2013 (T) not getting tagged as numeric

	truncate table tm_wz.wt_num_data_types;

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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert numeric data into WZ wt_num_data_types',rowCt,stepCt,'Done') into rtnCd;

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
	select czx_write_audit(jobId,databaseName,procedureName,'Reset mixed date/text data_types to T',rowCt,stepCt,'Done') into rtnCd;

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
	select czx_write_audit(jobId,databaseName,procedureName,'Updated data_type flag for numeric data_types',rowCt,stepCt,'Done') into rtnCd;
	
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
				end ,'(\\){2,}', '\','g');
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
	select czx_write_audit(jobId,databaseName,procedureName,'Updated leaf_node in wrk_clinical_data',rowCt,stepCt,'Done') into rtnCd;
	
	--	get distinct leaf nodes and name
	
	execute ('truncate table tm_wz.wt_trial_nodes');
	
	begin
	insert into tm_wz.wt_trial_nodes
	(leaf_node
	,node_name
	,data_type)
	select distinct leaf_node
		  ,tm_cz.parse_nth_value(leaf_node,length(leaf_node)-length(replace(leaf_node,'\','')),'\')
		  ,data_type
	from tm_wz.wrk_clinical_data;
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
	select czx_write_audit(jobId,databaseName,procedureName,'Insert distinct leaf_nodes to wt_trial_nodes',rowCt,stepCt,'Done') into rtnCd;	

	--	Check if any duplicate records of key columns (site_id, subject_id, visit_name, data_label, category_cd) for numeric data
	--	exist.  Raise error if yes
	
	execute ('truncate table tm_wz.wt_clinical_data_dups');
	
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for duplicate key columns',rowCt,stepCt,'Done') into rtnCd;
/*	
	begin
	insert into tm_wz.wt_clinical_data_dups
	(site_id
	,subject_id
	,visit_name
	,data_label
	,category_cd)
	select w.site_id, w.subject_id, w.visit_name, w.data_label
		  ,replace(replace(replace(w.category_cd,'VISITNAME',coalesce(w.visit_name,''))
				  ,'DATALABEL',coalesce(w.data_label,'')),'OBSERVATION',coalesce(w.obs_string,'')) as category_cd
	from tm_wz.wrk_clinical_data w
	where exists
		 (select 1 from tm_wz.wt_num_data_types t
		 where coalesce(w.category_cd,'@') = coalesce(t.category_cd,'@')
		   and coalesce(w.data_label,'@') = coalesce(t.data_label,'@')
		   and coalesce(w.visit_name,'@') = coalesce(t.visit_name,'@')
		  )
	  and w.visit_date is null
	group by w.site_id, w.subject_id, w.visit_name, w.data_label
			,replace(replace(replace(w.category_cd,'VISITNAME',coalesce(w.visit_name,''))
				,'DATALABEL',coalesce(w.data_label,'')),'OBSERVATION',coalesce(w.obs_string,''))
	having count(*) > 1;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Check for duplicate key columns',rowCt,stepCt,'Done') into rtnCd;
*/			  
	if rowCt > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Duplicate values found in key columns',rowCt,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;	
	end if;
	
/*	
	-- Build all needed leaf nodes in one pass for both numeric and text nodes
 
	execute ('truncate table tm_wz.wt_trial_nodes');
	
	begin
	insert into tm_wz.wt_trial_nodes
	(leaf_node
	,category_cd
	,visit_name
	,data_label
	,data_value
	,data_type
	,obs_string
	,valuetype_cd
	)
    select regexp_replace(
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
				end ,'(\\){2,}', '\','g') as leaf_node,
    a.category_cd,
    a.visit_name,
	a.data_label,
	case when a.data_type = 'T' then a.data_value else null end as data_value
    ,a.data_type
	,a.obs_string
	,max(a.valuetype_cd)
	from  tm_wz.wrk_clinical_data a
	group by regexp_replace(
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
				end ,'(\\){2,}', '\','g')
			,a.category_cd
			,a.visit_name
			,a.data_label
			,case when a.data_type = 'T' then a.data_value else null end 
			,a.data_type
			,a.obs_string;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Create leaf nodes for trial',rowCt,stepCt,'Done') into rtnCd;

	--	set node_name
	
	begin
	update tm_wz.wt_trial_nodes
	set node_name=tm_cz.parse_nth_value(leaf_node,length(leaf_node)-length(replace(leaf_node,'\','')),'\');
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated node name for leaf nodes',rowCt,stepCt,'Done') into rtnCd;
*/
	
	--	check if any node is a parent of another, all nodes must be children
	
	select count(*) into rowCt
	from tm_wz.wt_trial_nodes p
		,tm_wz.wt_trial_nodes c
	where c.leaf_node like p.leaf_node || '%' escape '~'
	  and c.leaf_node != p.leaf_node;
	stepCt := stepCt + 1;
	select czx_write_audit(jobId,databaseName,procedureName,'Check if node is parent of another node',rowCt,stepCt,'Done') into rtnCd;

	if rowCt > 0 then
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Found leaf node that is parent of another node',rowCt,stepCt,'Done') into rtnCd;	
		select tm_cz.czx_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;	
	end if;
		
	--	insert subjects into patient_dimension if needed
	
	execute ('truncate table tm_wz.wt_subject_info');

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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert subject information into temp table',rowCt,stepCt,'Done') into rtnCd;

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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete dropped subjects from patient_dimension',rowCt,stepCt,'Done') into rtnCd;

	--	update patients with changed information
	begin
	with nsi as (select t.usubjid, t.sex_cd, t.age_in_years_num, t.race_cd from tm_wz.wt_subject_info t) 
	update i2b2demodata.patient_dimension
	set sex_cd=nsi.sex_cd
	   ,age_in_years_num=nsi.age_in_years_num
	   ,race_cd=nsi.race_cd
	   from nsi
	where sourcesystem_cd = nsi.usubjid;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update subjects with changed demographics in patient_dimension',rowCt,stepCt,'Done') into rtnCd;

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
		raise notice 'errm: %', errorMessage;
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert new subjects into patient_dimension',rowCt,stepCt,'Done') into rtnCd;
		
	--	new bulk delete of unused nodes
	
	truncate table tm_wz.wt_del_nodes;
	stepCt := stepCt + 1;	
	select czx_write_audit(jobId,databaseName,procedureName,'Truncate table tm_wz.wt_del_nodes',0,stepCt,'Done') into rtnCd;
	
	begin
	insert into tm_wz.wt_del_nodes
	select l.c_fullname
		  ,l.c_basecode
	from i2b2 l
	where l.c_visualattributes like 'L%'
	  and l.c_fullname like topNode || '%' escape '~'
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
	select czx_write_audit(jobId,databaseName,procedureName,'Insert nodes into tm_wz.wt_del_nodes',rowCt,stepCt,'Done') into rtnCd;
	
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
		select czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from i2b2',rowCt,stepCt,'Done') into rtnCd;
		
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
		select czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from concept_dimension',rowCt,stepCt,'Done') into rtnCd;
		
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
		select czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from observation_fact',rowCt,stepCt,'Done') into rtnCd;
/*		
		--	delete de_concept_visit unused nodes
		
		delete from deapp.de_concept_visit f
		where f.concept_cd in (select distinct x.c_basecode as concept_cd from tm_wz.wt_del_nodes x);
		stepCt := stepCt + 1;	
		czx_write_audit(jobId,databaseName,procedureName,'Bulk delete nodes from de_concept_visit',rowCt,stepCt,'Done');
		commit;
*/		
	end if;	
	
	--	bulk insert leaf nodes
	begin
	with ncd as (select t.leaf_node, t.node_name from tm_wz.wt_trial_nodes t)
	update i2b2demodata.concept_dimension
	set name_char=ncd.node_name
	from ncd
	where concept_path = ncd.leaf_node;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Update name_char in concept_dimension for changed names',rowCt,stepCt,'Done') into rtnCd;

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
				,c.node_name::text as node_name
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted new leaf nodes into I2B2DEMODATA concept_dimension',rowCt,stepCt,'Done') into rtnCd;
	
	--	update i2b2 with name, data_type and xml for leaf nodes
	begin
	with ncd as (select t.leaf_node, t.node_name, t.data_type from tm_wz.wt_trial_nodes t)
	update i2b2metadata.i2b2
	set c_name=ncd.node_name
	   ,c_columndatatype='T'    -- force T until i2b2 respects c_columndatatype ncd.data_type
	   ,c_metadataxml=case when ncd.data_type = 'T'
					  then null
					  else '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'
					  end
	from ncd
	where c_fullname = ncd.leaf_node;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Updated name and data type in i2b2 if changed',rowCt,stepCt,'Done') into rtnCd;
			   
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
	)
    select distinct (length(c.concept_path) - coalesce(length(replace(c.concept_path, '\','')),0)) / length('\') - 2 + root_level
		  ,c.concept_path
		  ,c.name_char
		  ,'LA'
		  ,'N'
		  ,'CONCEPT_CD'
		  ,'CONCEPT_DIMENSION'
		  ,'CONCEPT_PATH'
		  ,c.concept_path
		  ,c.concept_path
		  ,etlDate
		  ,etlDate
		  ,etlDate
		  ,c.sourcesystem_cd
		  ,c.concept_cd
		  ,'LIKE'
		  ,'T'     -- force to T until i2b2 respects c_columndatatype t.data_type 
		  ,'trial:' || TrialID 
		  ,'@'
		  ,case when t.data_type = 'T' then null
		   else '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'
		   end
    from i2b2demodata.concept_dimension c
		,tm_wz.wt_trial_nodes t
    where c.concept_path = t.leaf_node
	  and not exists
		 (select 1 from i2b2metadata.i2b2 x
		  where c.concept_path = x.c_fullname);
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Inserted leaf nodes into I2B2METADATA i2b2',rowCt,stepCt,'Done') into rtnCd;

	--	delete from observation_fact all concept_cds for trial that are clinical data, exclude concept_cds from biomarker data
	
	begin
	delete from i2b2demodata.observation_fact f
	where f.sourcesystem_cd = TrialId
	  and f.concept_cd not in
		 (select distinct concept_code as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and concept_code is not null
		  union
		  select distinct platform_cd as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and platform_cd is not null
		  union
		  select distinct sample_type_cd as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and sample_type_cd is not null
		  union
		  select distinct tissue_type_cd as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and tissue_type_cd is not null
		  union
		  select distinct timepoint_cd as concept_cd from deapp.de_subject_sample_mapping
		  where trial_name = TrialId
		    and timepoint_cd is not null
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Delete clinical data for study from observation_fact',rowCt,stepCt,'Done') into rtnCd;	  
	
    --Insert into observation_fact
	
	--select tm_cz.czx_table_index_maint('DROP','i2b2demodata','observation_fact',jobId) into rtnCd;
	raise notice 'before observation_fact';
	
	begin
	insert into i2b2demodata.observation_fact
	(patient_num
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
	,instance_num
	,start_date
	,end_date
	)
	select distinct c.patient_num,
		   i.c_basecode,
		   '@',
		   --coalesce(vv.modifier_cd,'@'),
		   a.data_type,
		   case when a.data_type = 'T' then a.data_value
				else 'E'  --Stands for Equals for numeric types
				end,
		   case when a.data_type = 'N' then a.data_value::numeric
				else null --Null for text types
				end,
		   a.study_id, 
		   etlDate, 
		   '@',
		   '@',
		   '@'
		   --a.units_cd
		   ,1
		  --,row_number() over (partition by i.c_basecode, c.patient_num order by a.visit_date) as instance_num
		  ,to_date(a.visit_date,'YYYY/MM/DD HH24:MI')
		  ,to_date(a.end_date,'YYYY/MM/DD HH24:MI')
	from tm_wz.wrk_clinical_data a
		 inner join i2b2demodata.patient_dimension c
             on  a.usubjid = c.sourcesystem_cd
		 --inner join tm_wz.wt_trial_nodes t
         --    on  coalesce(a.category_cd,'@') = coalesce(t.category_cd,'@')
         --    and coalesce(a.data_label,'**NULL**') = coalesce(t.data_label,'**NULL**')
         --    and coalesce(a.visit_name,'**NULL**') = coalesce(t.visit_name,'**NULL**')
         --    and case when a.data_type = 'T' then a.data_value else '**NULL**' end = coalesce(t.data_value,'**NULL**')
		 inner join i2b2metadata.i2b2 i
             on a.leaf_node = i.c_fullname;
	--where a.data_value is not null
	--  and not exists		-- don't insert if lower level node exists
	--	 (select 1 from tm_wz.wt_trial_nodes x
	--	  where x.leaf_node like t.leaf_node || '%_' escape '`');
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert trial into I2B2DEMODATA observation_fact',rowCt,stepCt,'Done') into rtnCd;
	--select tm_cz.czx_table_index_maint('ADD','i2b2demodata','observation_fact',jobId) into rtnCd;
		
	-- fill in folder nodes 
  
	select tm_cz.i2b2_fill_in_tree(TrialId, topNode, jobID) into rtnCd;

	--	update c_visualattributes for all nodes in study, done to pick up node that changed c_columndatatype
	
	begin
	with upd as (select p.c_fullname, count(*) as nbr_children 
				 from tm_wz.wt_mrna_nodes t
				     ,i2b2metadata.i2b2 p
					 ,i2b2metadata.i2b2 c
				 where t.node_type = 'LEAF'
				   and t.leaf_node like p.c_fullname || '%' escape ''
				   and c.c_fullname like p.c_fullname || '%' escape '`'
				   and p.c_fullname > topNode
				   group by p.c_fullname)
	update i2b2metadata.i2b2 b
	set c_visualattributes=case when upd.nbr_children = 1 
								then 'L' || substr(b.c_visualattributes,2,2)
								else 'F' || substr(b.c_visualattributes,2,1) ||
									case when upd.c_fullname = topNode and highlight_study = 'Y'
										 then 'J' else substr(b.c_visualattributes,3,1) end
								end
	from upd
	where b.c_fullname = upd.c_fullname;
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Set c_visualattributes in i2b2',rowCt,stepCt,'Done') into rtnCd;
	
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
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Set sourcesystem_cd to null for added upper-level nodes',rowCt,stepCt,'Done') into rtnCd;

	select tm_cz.i2b2_create_concept_counts(topNode, jobID) into rtnCd;
	
	--	bulk delete hidden nodes
	
	truncate table tm_wz.wt_del_nodes;
	stepCt := stepCt + 1;	
	select czx_write_audit(jobId,databaseName,procedureName,'Truncate table tm_wz.wt_del_nodes for hidden nodes',0,stepCt,'Done') into rtnCd;
	
	begin
	insert into tm_wz.wt_del_nodes
	select l.c_fullname
		  ,l.c_basecode
	from i2b2metadata.i2b2 l
	where substr(l.c_visualattributes,2,1) = 'H'
	  and l.c_fullname like topNode || '%' escape '~';
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
	select czx_write_audit(jobId,databaseName,procedureName,'Insert hidden nodes into tm_wz.wt_del_nodes',rowCt,stepCt,'Done') into rtnCd;
	
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
		select czx_write_audit(jobId,databaseName,procedureName,'Bulk delete hidden nodes from i2b2',rowCt,stepCt,'Done') into rtnCd;
		
		--	delete concept_dimension unused nodes
		begin
		delete from concept_dimension f
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
		select czx_write_audit(jobId,databaseName,procedureName,'Bulk delete hidden nodes from concept_dimension',rowCt,stepCt,'Done') into rtnCd;

	end if;  	

	select tm_cz.i2b2_create_security_for_trial(TrialId, secureStudy, jobID) into rtnCd;
	select tm_cz.i2b2_load_security_data(jobID) into rtnCd;
	
	stepCt := stepCt + 1;
	select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'End i2b2_load_clinical_data',0,stepCt,'Done') into rtnCd;
	
	---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		select tm_cz.czx_end_audit (jobID, 'SUCCESS') into rtnCd;
	END IF;

	return 1;
/*	
	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
*/
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.i2b2_load_clinical_data(character varying, character varying, character varying, character varying, numeric) SET search_path=tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp;

ALTER FUNCTION tm_cz.i2b2_load_clinical_data(character varying, character varying, character varying, character varying, numeric)
  OWNER TO postgres;
