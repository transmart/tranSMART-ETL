set define off;

  CREATE OR REPLACE PROCEDURE "RWG_IMPORT_FROM_EXT" 
(
  trialID varchar2
 ,currentJobID NUMBER := null
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
  
BEGIN

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
  
  cz_write_audit(jobId,databaseName,procedureName,'Start Procedure',SQL%ROWCOUNT,stepCt,'Done');
  Stepct := Stepct + 1;	
  
  delete from TM_LZ.Rwg_Analysis where upper(study_id) =upper(trialID);
  
  cz_write_audit(jobId,databaseName,procedureName,'Delete existing records from TM_LZ.Rwg_Analysis',SQL%ROWCOUNT,stepCt,'Done');
  stepCt := stepCt + 1;	
  
 delete from TM_LZ.Rwg_Cohorts where upper(study_id) =upper(trialID);
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from TM_LZ.Rwg_Cohorts',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  
  commit;
  
   delete from TM_LZ.Rwg_Samples where upper(study_id) =upper(trialID);
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from TM_LZ.Rwg_Samples',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  
  commit;
    
  -- not used??
  -- delete from TM_LZ.RWG_BAAD_ID where upper(study_id) =upper(trialID);
  --Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from TM_LZ.RWG_BAAD_ID',Sql%Rowcount,Stepct,'Done');
  --stepCt := stepCt + 1;	
  --commit;
  
  
  
  --Insert Analysis
    INSERT  INTO TM_LZ.Rwg_Analysis
  (
    Study_Id,
    Cohorts,
  ANALYSIS_ID, 
  pvalue_cutoff, 
  foldchange_cutoff , 
  lsmean_cutoff,
    
    Analysis_Type,
    Data_Type,
    Platform,
    Long_Desc,
    Short_Desc,
    import_date
  )
 Select 
   Upper(Replace(  Study_Id,'"','')), 
   REGEXP_REPLACE(upper(Replace(  Cohorts,'"','')), '\s*', ''), 
   
  Replace(  ANALYSIS_ID ,'"',''), 
   Replace( pvalue_cutoff ,'"',''), 
  Replace(  foldchange_cutoff ,'"',''), 
   Replace( lsmean_cutoff ,'"',''), 
   
   Replace(  Analysis_Type,'"',''), 
   Replace(  Data_Type,'"',''), 
  Replace(   Platform,'"',''), 
   Replace(  Long_Desc,'"',''), 
  Replace(   short_desc,'"',''), 
    Sysdate
    From  TM_LZ.Rwg_Analysis_Ext
    where upper(study_id)=upper(trialID);

  cz_write_audit(jobId,databaseName,procedureName,'Insert records from TM_LZ.Rwg_Analysis_Ext to TM_LZ.Rwg_Analysis',SQL%ROWCOUNT,stepCt,'Done');
  stepCt := stepCt + 1;	
  
  commit;
  
    --Insert Cohorts
  INSERT
  INTO TM_LZ.Rwg_Cohorts
  (
    Study_Id,
    Cohort_Id,
    Cohort_Title, Disease, Long_Desc, 
    Organism, Pathology, Sample_Type, Short_Desc, Treatment,IMPORT_DATE
  )
  Select 
  Upper(Replace(  Study_Id,'"','')),
  trim(upper(Replace(  Cohort_Id,'"',''))),
  Replace(  Cohort_Title, '"',''),
  Replace(  Disease, '"',''),
  Replace(  Long_Desc, '"',''),
  Replace(  Organism, '"',''),
  Replace(  Pathology, '"',''),
  Replace(  Sample_Type, '"',''),
  Replace(  Short_Desc, '"',''),
  Replace(  Treatment,'"',''),
    Sysdate
    From  TM_LZ.Rwg_Cohorts_Ext
    where upper(study_id)=upper(trialID);

  cz_write_audit(jobId,databaseName,procedureName,'Insert records from TM_LZ.Rwg_Cohorts_Ext to TM_LZ.Rwg_Cohorts',SQL%ROWCOUNT,stepCt,'Done');
  stepCt := stepCt + 1;	
  
  commit;
  
  
  
   
    --Insert samples
  INSERT
  INTO TM_LZ.Rwg_Samples
  (
   study_id, COHORTS, EXPR_ID, IMPORT_DATE
  )
  Select 
  Upper(Replace(  Study_Id,'"','')),
  trim(upper(Replace(  Cohorts,'"',''))),
  Replace(  Expr_Id, '"',''),
  Sysdate
    From  TM_LZ.Rwg_Samples_Ext
    where upper(study_id)=upper(trialID);

  cz_write_audit(jobId,databaseName,procedureName,'Insert records from TM_LZ.Rwg_Samples_Ext to TM_LZ.Rwg_Samples',SQL%ROWCOUNT,stepCt,'Done');
  stepCt := stepCt + 1;	
  
  commit;
  
  
/*  not used??
 --Insert Bio_Assay_Analysis_ID
  INSERT
  INTO TM_LZ.rwg_baad_id
  (
    Study_Id,
    Cohorts,
    baad_id,
    import_date
  )
 Select 
   Upper(Replace(  Study_Id,'"','')), 
   REGEXP_REPLACE(upper(Replace(  Cohorts,'"','')), '\s*', ''), 
   Replace(  baad_id,'"',''), 
  
    Sysdate
    From  TM_LZ.rwg_baad_id_ext
    where upper(study_id)=upper(trialID);

  cz_write_audit(jobId,databaseName,procedureName,'Insert records from TM_LZ.rwg_baad_id_ext to TM_LZ.rwg_baad_id',SQL%ROWCOUNT,stepCt,'Done');
  stepCt := stepCt + 1;	
  
  commit;
*/  
  
  
  
  
  
 
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


