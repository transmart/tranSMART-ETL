alter table deapp.de_subject_rbm_data drop column rbm_annotation_id;

create or replace
PROCEDURE         "I2B2_LOAD_RBM_ANNOTATION" 
(
currentJobID NUMBER := null
 )
AS
/*************************************************************************
* This is for RBM Annotation ETL for Sanofi
* Date:12/05/2013
******************************************************************/

	--Audit variables
	newJobFlag INTEGER(1);
	databaseName VARCHAR(100);
	procedureName VARCHAR(100);
	jobID number(18,0);
	stepCt number(18,0);
	gplId	varchar2(100);

BEGIN

	stepCt := 0;

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

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_load_rbm_annotation',0,stepCt,'Done');

	--	get GPL id from external table
	
	select distinct gpl_id into gplId from LT_SRC_RBM_ANNOTATION;
	
	
	--	delete any existing data from antigen_deapp
	
	delete from antigen_deapp
	where platform = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from REFERENCE antigen_deapp',SQL%ROWCOUNT,stepCt,'Done');

		
	--	delete any existing data from annotation_deapp
	
	delete from annotation_deapp
	where gpl_id = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from annotation_deapp',SQL%ROWCOUNT,stepCt,'Done');

	--	delete any existing data from deapp.de_mrna_annotation
	
	delete from deapp.DE_RBM_ANNOTATION
	where gpl_id = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from de_mrna_annotation',SQL%ROWCOUNT,stepCt,'Done');

	--	update organism for existing probesets in probeset_deapp
	/*
	update probeset_deapp p  --check
	set organism=(select distinct t.organism from LT_SRC_RBM_ANNOTATION t
				  where p.platform = t.gpl_id
				    and p.probeset = t.probe_id)
	where exists
		 (select 1 from LT_SRC_RBM_ANNOTATION x
		  where p.platform = x.gpl_id
		    and p.probeset = x.probe_id);
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update organism in probeset_deapp',SQL%ROWCOUNT,stepCt,'Done');
		*/	
	--	insert any new probesets into probeset_deapp
	
	insert into antigen_deapp 
	(antigen_name
	,platform)
	select distinct antigen_name
	      ,gpl_id
	from LT_SRC_RBM_ANNOTATION t
	where not exists
		 (select 1 from antigen_deapp x
		  where t.gpl_id = x.platform
		    and t.antigen_name = x.antigen_name
		);
	--where gene_id is not null 
	--   or gene_symbol is not null;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert new probesets into antigen_deapp',SQL%ROWCOUNT,stepCt,'Done');
		
	--	insert data into annotation_deapp
	
	insert into annotation_deapp
	(gpl_id
	,probe_id
	,gene_symbol
	,gene_id
	,probeset_id
	,organism)
	select distinct d.gpl_id
	,d.uniprotid
	,d.gene_symbol
	,d.gene_id
	,p.antigen_id
	,'Homo sapiens'
	from LT_SRC_RBM_ANNOTATION d
	,antigen_deapp p
	where d.antigen_name = p.antigen_name
	  and d.gpl_id = p.platform
	  and (d.gene_id is not null or d.gene_symbol is not null) ;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load annotation data into REFERENCE annotation_deapp',SQL%ROWCOUNT,stepCt,'Done');
		
	--	insert data into deapp.de_rbm_annotation
	
	insert into de_rbm_annotation
	(gpl_id
        ,id
	,antigen_name
        ,uniprot_id
	,gene_symbol
	,gene_id
	) 
        SELECT d.gpl_id,p.antigen_id,d.antigen_name,trim(d.uniprotid) as uniprotid,d.gene_symbol,decode(d.gene_id,null,null,to_number(d.gene_id)) as gene_id FROM (
        SELECT DISTINCT REGEXP_SUBSTR (uniprotid, '[^,]+', 1, RN) AS uniprotid,antigen_name,gene_id,gpl_id,gene_symbol
        FROM TM_LZ.LT_SRC_RBM_ANNOTATION 
        CROSS JOIN (SELECT ROWNUM AS RN 
        FROM (SELECT MAX(REGEXP_COUNT(uniprotid,',') + 1) MX FROM TM_LZ.LT_SRC_RBM_ANNOTATION) 
        CONNECT BY LEVEL <= MX) 
        WHERE REGEXP_SUBSTR (uniprotid, '[^,]+', 1, RN) IS NOT NULL 
        ORDER BY 1
        )d
        ,antigen_deapp p 
	where d.antigen_name = p.antigen_name
        and d.gpl_id = p.platform;
        
	commit;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load annotation data into DEAPP de_rbm_annotation',SQL%ROWCOUNT,stepCt,'Done');
		
	--	update gene_id if null
	
	update de_rbm_annotation t
	set gene_id=(select to_number(min(b.primary_external_id)) as gene_id
				 from biomart.bio_marker b
				 where t.gene_symbol = b.bio_marker_name
				  -- and upper(b.organism) = upper(t.organism)
				   and upper(b.bio_marker_type) = 'RBM')
	where t.gpl_id = gplId
	  and t.gene_id is null
	  and t.gene_symbol is not null
	  and exists
		 (select 1 from biomart.bio_marker x
		  where t.gene_symbol = x.bio_marker_name
			--and upper(x.organism) = upper(t.organism)
			and upper(x.bio_marker_type) = 'RBM');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated missing gene_id in de_rbm_annotation',SQL%ROWCOUNT,stepCt,'Done');
	
	--	update gene_symbol if null
	
	update de_rbm_annotation t 
	set gene_symbol=(select min(b.bio_marker_name) as gene_symbol
				 from biomart.bio_marker b
				 where to_char(t.gene_id) = b.primary_external_id
				 --  and upper(b.organism) = upper(t.organism)
				   and upper(b.bio_marker_type) = 'RBM')
	where t.gpl_id = gplId
	  and t.gene_symbol is null
	  and t.gene_id is not null
	  and exists
		 (select 1 from biomart.bio_marker x
		  where to_char(t.gene_id) = x.primary_external_id
			--and upper(x.organism) = upper(t.organism)
			and upper(x.bio_marker_type) = 'RBM');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated missing gene_id in de_rbm_annotation',SQL%ROWCOUNT,stepCt,'Done');
	
	--	insert probesets into biomart.bio_assay_feature_group
	
	insert into biomart.bio_assay_feature_group
	(feature_group_name
	,feature_group_type)
	select distinct t.uniprotid, 'PROTEIN'
	from tm_lz.LT_SRC_RBM_ANNOTATION t
	where not exists
		 (select 1 from biomart.bio_assay_feature_group x
		  where t.uniprotid = x.feature_group_name)
		and t.uniprotid is not null;
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert probesets into biomart.bio_assay_feature_group',SQL%ROWCOUNT,stepCt,'Done');
		  
	--	insert probesets into biomart.bio_assay_data_annotation
	
	insert into biomart.bio_assay_data_annotation
	(bio_assay_feature_group_id
	,bio_marker_id)
	select distinct fg.bio_assay_feature_group_id
		  ,coalesce(bgs.bio_marker_id,bgi.bio_marker_id)
	from LT_SRC_RBM_ANNOTATION t
		,biomart.bio_assay_feature_group fg
		,biomart.bio_marker bgs
		,biomart.bio_marker bgi
	where (t.gene_symbol is not null or t.gene_id is not null)
	  and t.uniprotid = fg.feature_group_name
	  and t.gene_symbol = bgs.bio_marker_name(+)
	--  and upper(coalesce(t.organism,'Homo sapiens')) = upper(bgs.organism)
	  and to_char(t.gene_id) = bgi.primary_external_id(+)
	 -- and upper(coalesce(t.organism,'Homo sapiens')) = upper(bgi.organism)
	  and coalesce(bgs.bio_marker_id,bgi.bio_marker_id,-1) > 0
	  and not exists 
		 (select 1 from biomart.bio_assay_data_annotation x
		  where fg.bio_assay_feature_group_id = x.bio_assay_feature_group_id
		    and coalesce(bgs.bio_marker_id,bgi.bio_marker_id,-1) = x.bio_marker_id);
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Link feature_group to bio_marker in biomart.bio_assay_data_annotation',SQL%ROWCOUNT,stepCt,'Done');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_load_rbm_annotation',0,stepCt,'Done');
	
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

END;

create or replace
PROCEDURE         "I2B2_RBM_ZSCORE_CALC_NEW" 
(
  trial_id VARCHAR2
 ,run_type varchar2 := 'L'
 ,currentJobID NUMBER := null
 ,data_type varchar2 := 'R'
 ,log_base	number := 2
 ,source_cd	varchar2
)
AS
/*************************************************************************
This Stored Procedure is used in ETL load RBM data
Date:12/04/2013
******************************************************************/

  TrialID varchar2(50);
  sourceCD	varchar2(50);
  sqlText varchar2(2000);
  runType varchar2(10);
  dataType varchar2(10);
  stgTrial varchar2(50);
  idxExists number;
  pExists	number;
  nbrRecs number;
  logBase number;
   
  --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  
  --  exceptions
  invalid_runType exception;
  trial_mismatch exception;
  trial_missing exception;
  
BEGIN

	TrialId := trial_id;
	runType := run_type;
	dataType := data_type;
	logBase := log_base;
	sourceCd := source_cd;
	  
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
  
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Starting zscore calc for ' || TrialId || ' RunType: ' || runType || ' dataType: ' || dataType,0,stepCt,'Done');
  
	if runType != 'L' then
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Invalid runType passed - procedure exiting',SQL%ROWCOUNT,stepCt,'Done');
		raise invalid_runType;
	end if;
  
--	For Load, make sure that the TrialId passed as parameter is the same as the trial in stg_subject_mrna_data
--	If not, raise exception

	if runType = 'L' then
		select distinct trial_name into stgTrial
		from WT_SUBJECT_RBM_PROBESET;
		
		if stgTrial != TrialId then
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'TrialId not the same as trial in WT_SUBJECT_RBM_PROBESET - procedure exiting',SQL%ROWCOUNT,stepCt,'Done');
			raise trial_mismatch;
		end if;
	end if;

	--remove Reload processing
--	For Reload, make sure that the TrialId passed as parameter has data in de_subject_rbm_data
--	If not, raise exception

	if runType = 'R' then 
		select count(*) into idxExists
		from de_subject_rbm_data
		where trial_name = TrialId;
		
		if idxExists = 0 then
			stepCt := stepCt + 1;
			cz_write_audit(jobId,databaseName,procedureName,'No data for TrialId in de_subject_rbm_data - procedure exiting',SQL%ROWCOUNT,stepCt,'Done');
			raise trial_missing;
		end if;
	end if;

   
--	truncate tmp tables

	execute immediate('truncate table tm_wz.wt_subject_rbm_logs');
	execute immediate('truncate table tm_wz.wt_subject_rbm_calcs');
	execute immediate('truncate table tm_wz.wt_subject_rbm_med');

	select count(*) 
	into idxExists
	from all_indexes
	where table_name = 'WT_SUBJECT_RBM_LOGS'
	  and index_name = 'WT_SUBJECT_RBM_LOGS_I1'
	  and owner = 'TM_WZ';
		
	if idxExists = 1 then
		execute immediate('drop index tm_wz.wt_subject_rbm_logs_i1');		
	end if;
	
	select count(*) 
	into idxExists
	from all_indexes
	where table_name = 'WT_SUBJECT_RBM_CALCS'
	  and index_name = 'WT_SUBJECT_RBM_CALCS_I1'
	  and owner = 'TM_WZ';
		
	if idxExists = 1 then
		execute immediate('drop index tm_wz.wt_subject_rbm_calcs_i1');
	end if;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Truncate work tables in TM_WZ',0,stepCt,'Done');
	
	--	if dataType = L, use intensity_value as log_intensity
	--	if dataType = R, always use intensity_value


	if dataType = 'L' then
		insert into wt_subject_rbm_logs 
			(probeset_id
			,intensity_value
			,assay_id
			,log_intensity
			,patient_id
		--	,sample_cd
		--	,subject_id
			)
			select probeset
				  ,intensity_value  
				  ,assay_id 
				  ,intensity_value
				  ,patient_id
			--	  ,sample_cd
			--	  ,subject_id
			from wt_subject_rbm_probeset
			where trial_name = TrialId;
           
		--end if;
	else	
                	insert into wt_subject_rbm_logs 
			(probeset_id
			,intensity_value
			,assay_id
			,log_intensity
			,patient_id
		--	,sample_cd
		--	,subject_id
			)
			select probeset
				  ,intensity_value 
				  ,assay_id 
				  ,log(2,intensity_value)
				  ,patient_id
		--		  ,sample_cd
		--		  ,subject_id
			from wt_subject_rbm_probeset
			where trial_name = TrialId;
--		end if;

	end if;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Loaded data for trial in TM_WZ wt_subject_rbm_logs',SQL%ROWCOUNT,stepCt,'Done');

	commit;
    
	execute immediate('create index tm_wz.wt_subject_rbm_logs_i1 on tm_wz.wt_subject_rbm_logs (trial_name, probeset_id) nologging  tablespace "INDX"');
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create index on TM_WZ wt_subject_rbm_logs',0,stepCt,'Done');
		
--	calculate mean_intensity, median_intensity, and stddev_intensity per experiment, probe

	insert into wt_subject_rbm_calcs
	(trial_name
	,probeset_id
	,mean_intensity
	,median_intensity
	,stddev_intensity
	)
	select d.trial_name 
		  ,d.probeset_id
		  ,avg(log_intensity)
		  ,median(log_intensity)
		  ,stddev(log_intensity)
	from wt_subject_rbm_logs d 
	group by d.trial_name 
			,d.probeset_id;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Calculate intensities for trial in TM_WZ wt_subject_rbm_calcs',SQL%ROWCOUNT,stepCt,'Done');

	commit;

	execute immediate('create index tm_wz.wt_subject_rbm_calcs_i1 on tm_wz.wt_subject_rbm_calcs (trial_name, probeset_id) nologging tablespace "INDX"');
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Create index on TM_WZ wt_subject_rbm_calcs',0,stepCt,'Done');
		
-- calculate zscore

	insert into wt_subject_rbm_med parallel 
	(probeset_id
	,intensity_value
	,log_intensity
	,assay_id
	,mean_intensity
	,stddev_intensity
	,median_intensity
	,zscore
	,patient_id
--	,sample_cd
--	,subject_id
	)
	select d.probeset_id
		  ,d.intensity_value 
		  ,d.log_intensity 
		  ,d.assay_id  
		  ,c.mean_intensity 
		  ,c.stddev_intensity 
		  ,c.median_intensity 
		  ,(CASE WHEN stddev_intensity=0 THEN 0 ELSE (log_intensity - median_intensity ) / stddev_intensity END)
		  ,d.patient_id
	--	  ,d.sample_cd
	--	  ,d.subject_id
    from wt_subject_rbm_logs d 
		,wt_subject_rbm_calcs c 
    where d.probeset_id = c.probeset_id;
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Calculate Z-Score for trial in TM_WZ wt_subject_rbm_med',SQL%ROWCOUNT,stepCt,'Done');

    commit;

/*
	select count(*) into n
	select count(*) into nbrRecs
	from wt_subject_rbm_med;
	
	if nbrRecs > 10000000 then
		i2b2_mrna_index_maint('DROP',,jobId);
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Drop indexes on DEAPP de_subject_rbm_data',0,stepCt,'Done');
	else
		stepCt := stepCt + 1;
		cz_write_audit(jobId,databaseName,procedureName,'Less than 10M records, index drop bypassed',0,stepCt,'Done');
	end if;
*/	


	insert into de_subject_rbm_data 
	(trial_name
        --,rbm_annotation_id
	,antigen_name
	,patient_id
        ,gene_symbol
        ,gene_id
        ,assay_id
        ,concept_cd
        ,value
        ,normalized_value
        ,unit    
	,zscore
	)
	select TrialId
               --,a.id ---rbm_annotation_id
              ,trim(substr(m.probeset_id,1,instr(m.probeset_id,'(')-1)) --m.probeset_id 
              ,m.patient_id
              ,a.gene_symbol  -- gene-symbol 
              ,a.gene_id  --gene_id
              ,m.assay_id
              ,d.concept_code --concept_cd
              ,m.intensity_value
              ,round(case when dataType = 'R' then m.intensity_value
				when dataType = 'L' 
				then case when logBase = -1 then null else power(logBase, m.log_intensity) end
				else null
				end,4) as normalized_value
	    --  ,decode(dataType,'R',m.intensity_value,'L',power(logBase, m.log_intensity),null)
              
              ,trim(substr(m.probeset_id ,instr(m.probeset_id ,'(',-1,1),length(m.probeset_id )))
              ,(CASE WHEN m.zscore < -2.5 THEN -2.5 WHEN m.zscore >  2.5 THEN  2.5 ELSE m.zscore END)
          --	  ,m.sample_id
	--	  ,m.subject_id
	from wt_subject_rbm_med m
        ,wt_subject_rbm_probeset p
        ,DE_RBM_ANNOTATION a
        ,de_subject_sample_mapping d
        where 
        trim(substr(p.probeset,1,instr(p.probeset,'(')-1)) =trim(a.antigen_name) 
        --and 
      and   d.subject_id=p.subject_id
        and p.platform=a.gpl_id
        and m.assay_id=p.assay_id
        and d.platform=p.platform
        and d.patient_id=p.patient_id
        and d.concept_code in (select concept_cd from  i2b2demodata.concept_dimension where concept_cd=d.concept_code)
        and d.trial_name=TrialId
        
        and p.patient_id=m.patient_id
        and p.probeset=m.probeset_id;
        
        /*
        select distinct TrialId
               ,a.id ---rbm_annotation_id
              ,trim(substr(m.probeset_id,1,instr(m.probeset_id,'(')-1)) --m.probeset_id 
              ,m.patient_id
              ,a.gene_symbol  -- gene-symbol 
              ,a.gene_id  --gene_id
              ,m.assay_id
              ,d.concept_code --concept_cd
              ,m.intensity_value
              ,round(case when dataType = 'R' then m.intensity_value
				when dataType = 'L' 
				then case when logBase = -1 then null else power(logBase, m.log_intensity) end
				else null
				end,4) as normalized_value
	    --  ,decode(dataType,'R',m.intensity_value,'L',power(logBase, m.log_intensity),null)
              
              ,trim(substr(m.probeset_id ,instr(m.probeset_id ,'('),length(m.probeset_id )))
              ,(CASE WHEN m.zscore < -2.5 THEN -2.5 WHEN m.zscore >  2.5 THEN  2.5 ELSE m.zscore END)
          --	  ,m.sample_id
	--	  ,m.subject_id
         from wt_subject_rbm_med m, DE_RBM_ANNOTATION a,de_subject_sample_mapping d
         --(select distinct intensity_value from wt_subject_rbm_probeset p) x
        where a.antigen_name = trim(substr(m.probeset_id,1,instr(m.probeset_id,'(')-1)) and m.patient_id = d.patient_id  
        --and d.assay_id  = m.assay_id
        and d.trial_name = TrialId
        and d.source_cd = source_cd;
        --and d.gpl_id = a.gpl_id;
        --and x.intensity_value = m.intensity_value;
    */
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert data for trial in DEAPP de_subject_rbm_data',SQL%ROWCOUNT,stepCt,'Done');

  	commit;

--	add indexes, if indexes were not dropped, procedure will not try and recreate
/*
	i2b2_mrna_index_maint('ADD',,jobId);
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Add indexes on DEAPP de_subject_rbm_data',0,stepCt,'Done');
*/
	
--	cleanup tmp_ files

	--execute immediate('truncate table tm_wz.wt_subject_rbm_logs');
	--execute immediate('truncate table tm_wz.wt_subject_rbm_calcs');
	--execute immediate('truncate table tm_wz.wt_subject_rbm_med');

   	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Truncate work tables in TM_WZ',0,stepCt,'Done');
    
    ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN 
    cz_end_audit (jobID, 'SUCCESS'); 
  END IF;

  EXCEPTION

  WHEN invalid_runType or trial_mismatch or trial_missing then
    --Handle errors.
    cz_error_handler (jobID, procedureName);
    --End Proc
  
    cz_end_audit (jobID, 'FAIL');
  when OTHERS THEN
    --Handle errors.
    cz_error_handler (jobID, procedureName);


    cz_end_audit (jobID, 'FAIL');
	
END;
