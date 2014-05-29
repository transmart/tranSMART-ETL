set define off;

  CREATE OR REPLACE PROCEDURE "RWG_LOAD_ANALYSIS_DATA" 
(
  trialID varchar2
 ,currentJobID NUMBER := null
 ,inPlatformID number := null
 ,rtn_code	   OUT number
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

  --Audit variables
  newJobFlag INTEGER(1);
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID number(18,0);
  stepCt number(18,0);
  
  
  vWZcount integer;
  vLZcount integer;
  vPlatformID integer;
  vExpID integer;
  
   Cursor Cdelete Is 
   Select distinct(bio_assay_analysis_id)
   from tm_lz.Rwg_Analysis
   -- From BIOMART.bio_analysis_cohort_xref
    where upper(study_id) = upper(trialID);     
    
    cDeleteRow cDelete%rowtype;

BEGIN

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;
  rtn_code := 0;


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
  
  cz_write_audit(jobId,databaseName,procedureName,'Start Procedure',SQL%ROWCOUNT,stepCt,'Done');
  Stepct := Stepct + 1;	
  


  For Cdeleterow In Cdelete Loop
    Delete From BIOMART.bio_assay_analysis_data
    where bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;
          
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete records from BIOMART.bio_assay_analysis_data for analysis:  ' || cDeleteRow.bio_assay_analysis_id,Sql%Rowcount,Stepct,'Done');
    stepCt := stepCt + 1;	
    
    
    Delete From tm_lz.rwg_analysis_data 
    where bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;
          
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete records from tm_lz.rwg_analysis_data for analysis:  ' || cDeleteRow.bio_assay_analysis_id,Sql%Rowcount,Stepct,'Done');
    stepCt := stepCt + 1;	
      
      
    commit;
  end loop;
  
  
  execute immediate('truncate table tm_wz.bio_assay_analysis_data_new');
  cz_write_audit(jobId,databaseName,procedureName,'Truncate tm_wz.bio_assay_analysis_data_new',0,stepCt,'Done');
  stepCt := stepCt + 1;	
  commit;


  execute immediate('truncate table tm_wz.tmp_assay_analysis_metrics');
  cz_write_audit(jobId,databaseName,procedureName,'Truncate tm_wz.tmp_assay_analysis_metrics',0,stepCt,'Done');
  stepCt := stepCt + 1;	
  commit;


  -- not used ???  
  --delete from tm_lz.RWG_BAAD_ID where upper(study_id) =upper(trialID);
  --Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from tm_lz.RWG_BAAD_ID',Sql%Rowcount,Stepct,'Done');
  --stepCt := stepCt + 1;	
  --commit;
  
if (inPlatformID is null)
THEN

  select max(bap.bio_assay_platform_id) into vPlatformID
  from DEAPP.de_subject_sample_mapping ssm, DEAPP.de_gpl_info gpl, BIOMART.bio_assay_platform bap
  where upper(ssm.gpl_id) = upper(gpl.platform)
  and upper(ssm.trial_name) = upper(trialID)
  and ssm.platform = 'MRNA_AFFYMETRIX'
  and upper(bap.platform_accession) like '%'|| upper(gpl.platform) || '%';
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Get bio_assay_platform_ID: ' || vPlatformID,Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
ELSE
  
  vPlatformID := inPlatformID;
  
END IF;
  
  select exp.bio_experiment_id into vExpID 
  from BIOMART.bio_experiment exp 
  where upper(accession) = upper(trialID);
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Get bio_experiment_id: ' || vExpID,Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
  
  select count(*) into vLZcount from TM_LZ.RWG_ANALYSIS_DATA_EXT;
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Count for TM_LZ.RWG_ANALYSIS_DATA_EXT = ' || vLZcount,0,Stepct,'Done');
  stepCt := stepCt + 1;	
  
  --	count number of data records with non-numeric data in preferred_pvalue or fold_change and log
  
  select count(*) into vLZcount
  from tm_lz.rwg_analysis_data_ext
  where is_number(preferred_pvalue) = 1
	 or is_number(fold_change) = 1;

  cz_Write_Audit(Jobid,Databasename,Procedurename,'Data records dropped for non-numeric preferred_pvalue or fold_change',vLZcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;	 
	 
  --	insert data into rwg_analysis_data, skip records with non-numeric data in preferred_pvalue or fold_change
  --	change all other non-numeric data to null
  
  insert into  tm_lz.rwg_analysis_data 
  (study_id
  ,probeset
  ,fold_change
  ,pvalue
  ,raw_pvalue
  ,adjusted_pvalue
  ,min_lsmean
  ,max_lsmean
  ,analysis_cd
  ,bio_assay_analysis_id)
  select rwg.study_id
		,ext.probeset
		,ext.fold_change
		,ext.preferred_pvalue
		,case when is_number(ext.raw_pvalue) = 1 then null else ext.raw_pvalue end
		,case when is_number(ext.adjusted_pvalue) = 1 then null else ext.adjusted_pvalue end
		,case when is_number(ext.lsmean_1) = 1 or is_number(ext.lsmean_1) = 1 then null
			  when ext.lsmean_1>ext.lsmean_2 then ext.lsmean_2 
			  else ext.lsmean_1 end --min
		,case when is_number(ext.lsmean_1) = 1 or is_number(ext.lsmean_1) = 1 then null
			  when ext.lsmean_1>ext.lsmean_2 then ext.lsmean_1 
			  else ext.lsmean_2 end --max
		,ext.analysis_id
		,rwg.bio_assay_analysis_id
  from TM_LZ.RWG_ANALYSIS_DATA_EXT ext
	  ,tm_lz.rwg_analysis rwg
  where trim(upper(ext.analysis_id)) = trim(upper(rwg.analysis_id))
    and upper(rwg.study_id) = upper(trialID)
    and is_number(ext.preferred_pvalue) = 0 
	and is_number(ext.fold_change) = 0;

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert records into rwg_analysis_data',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
  
  select count(*) into vWZcount from tm_lz.rwg_analysis_data
  where study_id = upper(trialID);
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Count for tm_lz.rwg_analysis_data = ' || vWZcount,0,Stepct,'Done');
  stepCt := stepCt + 1;	
  
  
  insert into tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW(fold_change_ratio, raw_pvalue, adjusted_pvalue, 
  bio_assay_analysis_id, feature_group_name, bio_experiment_id, bio_assay_platform_id, 
  etl_id, preferred_pvalue,lsmean1, lsmean2, bio_assay_feature_group_id)
  select rad.fold_change, rad.raw_pvalue,rad.adjusted_pvalue, rad.bio_assay_analysis_id, rad.probeset, vExpID, vPlatformID,
  rad.study_id || ':RWG',rad.pvalue, rad.min_lsmean, rad.max_lsmean, bafg.bio_assay_feature_group_id
  from  tm_lz.rwg_analysis_data rad, BIOMART.bio_assay_feature_group bafg
  where rad.study_id = upper(trialID)		-- 20121212	JEA
    and rad.probeset = bafg.feature_group_name(+);
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert records into BIO_ASSAY_ANALYSIS_DATA_NEW',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
  
  
  
  /*Calculate TEA Values */
  
    
  insert into tm_wz.tmp_assay_analysis_metrics 
  select ad.bio_assay_analysis_id, count(*) data_ct, 
         avg(ad.fold_change_ratio) fc_mean, 
         Stddev(Ad.Fold_Change_Ratio) Fc_Stddev
  from tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW ad join biomart.bio_assay_analysis a 
     on ad.bio_assay_analysis_id = a.bio_assay_analysis_id
  where ad.fold_change_ratio is not null and a.bio_assay_data_type <> 'RBM'
  group by ad.bio_assay_analysis_id
  order by data_ct;
    
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert records into tm_wz.tmp_assay_analysis_metrics ',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
  
    
merge into tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW d
using tm_wz.tmp_assay_analysis_metrics m 
   on (d.bio_assay_analysis_id = m.bio_assay_analysis_id and d.fold_change_ratio is not null)
when matched then
   update set d.tea_normalized_pvalue = TEA_NPV_PRECOMPUTE(d.fold_change_ratio, m.fc_mean, m.fc_stddev);
    
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update TEA records in tm_lz.BIO_ASSAY_ANALYSIS_DATA_NEW',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  commit;
  
  /* Final Insert */
  
   For Cdeleterow In Cdelete Loop
   
      insert into biomart.bio_assay_analysis_data(
      FOLD_CHANGE_RATIO, RAW_PVALUE,ADJUSTED_PVALUE,BIO_ASSAY_ANALYSIS_ID,
      FEATURE_GROUP_NAME,BIO_EXPERIMENT_ID,BIO_ASSAY_PLATFORM_ID ,
      ETL_ID,PREFERRED_PVALUE,TEA_NORMALIZED_PVALUE,BIO_ASSAY_FEATURE_GROUP_ID,
      LSMEAN1,LSMEAN2 )
      select 
      FOLD_CHANGE_RATIO,RAW_PVALUE,ADJUSTED_PVALUE,BIO_ASSAY_ANALYSIS_ID,
      FEATURE_GROUP_NAME,BIO_EXPERIMENT_ID,BIO_ASSAY_PLATFORM_ID ,
      ETL_ID,PREFERRED_PVALUE,TEA_NORMALIZED_PVALUE,BIO_ASSAY_FEATURE_GROUP_ID,
      LSMEAN1,LSMEAN2 
      from tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW
      where bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;
                
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert records into biomart.bio_assay_analysis_data for analysis:  ' || cDeleteRow.bio_assay_analysis_id,Sql%Rowcount,Stepct,'Done');
      --Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert records into biomart.bio_assay_analysis_data',Sql%Rowcount,Stepct,'Done');
	  stepCt := stepCt + 1;	
      commit;
      
  end loop;
  
  cz_write_audit(jobId,databaseName,procedureName,'Procedure Complete',SQL%ROWCOUNT,stepCt,'Done');
  Stepct := Stepct + 1;	
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
	rtn_code := 16;
  
END;

