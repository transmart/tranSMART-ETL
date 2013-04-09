-- Function: tm_cz.rwg_load_analysis_data(character varying, numeric, character varying)

-- DROP FUNCTION tm_cz.rwg_load_analysis_data(character varying, numeric, character varying);

CREATE OR REPLACE FUNCTION tm_cz.rwg_load_analysis_data(trialid character varying, currentjobid numeric DEFAULT (-1), in_platform_name character varying DEFAULT ''::character varying)
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
	rtnCd			numeric;
	
	
  vWZcount integer;
  vLZcount integer;
  vPlatformID integer;
  vExpID integer;
  
   Cdelete CURSOR for 
   Select distinct(bio_assay_analysis_id)
   from tm_wz.Rwg_Analysis
   where upper(study_id) = upper(trialID);
   

BEGIN

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := 'TM_CZ';
	procedureName := 'RWG_LOAD_ANALYSIS_DATA';
	
	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select tm_cz.cz_start_audit (procedureName, databaseName) into jobId;
	END IF;




  For Cdeleterow In Cdelete Loop
    Delete From BIOMART.bio_assay_analysis_data
    where bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;

    get diagnostics rowCt := ROW_COUNT;
    
    select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete records from BIOMART.bio_assay_analysis_data for analysis:  ' || cDeleteRow.bio_assay_analysis_id,rowCt,Stepct,'Done') into rtnCd;
    stepCt := stepCt + 1;	
    

    Delete From tm_lz.lt_rwg_analysis_data
    where bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;

    get diagnostics rowCt := ROW_COUNT;
          
    select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete records from tm_lz.lt_rwg_analysis_datafor analysis:  ' || cDeleteRow.bio_assay_analysis_id,rowCt,Stepct,'Done') into rtnCd;
    stepCt := stepCt + 1;	
      
      
    
  end loop;
  
  
  execute 'truncate table tm_wz.bio_assay_analysis_data_new';
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Truncate tm_wz.bio_assay_analysis_data_new',0,stepCt,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  


  execute 'truncate table tm_wz.tmp_assay_analysis_metrics';
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Truncate tm_wz.tmp_assay_analysis_metrics',0,stepCt,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  


/*
  select max(bap.bio_assay_platform_id) into vPlatformID
  from DEAPP.de_subject_sample_mapping ssm, DEAPP.de_gpl_info gpl, BIOMART.bio_assay_platform bap
  where upper(ssm.gpl_id) = upper(gpl.platform)
  and upper(ssm.trial_name) = upper(trialID)
  and ssm.platform = 'MRNA_AFFYMETRIX'
  and upper(bap.platform_accession) like '%'|| upper(gpl.platform) || '%';
*/


  select max(bio_assay_platform_id) into vPlatformID
  from  biomart.bio_assay_platform 
  where platform_accession like in_platform_name;

  get diagnostics rowCt := ROW_COUNT;
  
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Get bio_assay_platform_ID: ' || vPlatformID,rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  select exp.bio_experiment_id into vExpID 
  from BIOMART.bio_experiment exp 
  where upper(accession) = upper(trialID);

  get diagnostics rowCt := ROW_COUNT;
  
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Get bio_experiment_id: ' || vExpID,rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  select count(*) into vLZcount from tm_lz.lt_rwg_analysis_data;
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Count for tm_lz.lt_rwg_analysis_data = ' || vLZcount,0,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  --	count number of data records with non-numeric data in preferred_pvalue or fold_change and log
  
  select count(*) into vLZcount
  from  tm_lz.lt_rwg_analysis_data
  where is_numeric(pvalue) = 1
	 or is_numeric(fold_change) = 1;

  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Data records dropped for non-numeric pvalue or fold_change',vLZcount,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  	 
	 
  --	insert data into rwg_analysis_data, skip records with non-numeric data in preferred_pvalue or fold_change
  --	change all other non-numeric data to null


  
insert into  tm_wz.rwg_analysis_data
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
		,ext.pvalue
		,case when is_numeric(ext.raw_pvalue) = 1 then null else ext.raw_pvalue end
		,case when is_numeric(ext.adjusted_pvalue) = 1 then null else ext.adjusted_pvalue end
		,case when is_numeric(ext.min_lsmean) = 1 or is_numeric(ext.min_lsmean) = 1 then null
			  when ext.min_lsmean>ext.max_lsmean then ext.max_lsmean 
			  else ext.min_lsmean end --min
		,case when is_numeric(ext.min_lsmean) = 1 or is_numeric(ext.min_lsmean) = 1 then null
			  when ext.min_lsmean>ext.max_lsmean then ext.min_lsmean 
			  else ext.max_lsmean end --max
		,ext.analysis_cd
		,rwg.bio_assay_analysis_id
  from tm_lz.lt_rwg_analysis_data ext
	  ,tm_wz.rwg_analysis rwg
  where trim(upper(ext.analysis_cd)) = trim(upper(rwg.analysis_id))
    and upper(rwg.study_id) = upper(trialID)
    and is_numeric(ext.pvalue) = 0 
	and is_numeric(ext.fold_change) = 0;


	get diagnostics rowCt := ROW_COUNT;

  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into rwg_analysis_data',rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  select count(*) into vWZcount from tm_lz.lt_rwg_analysis_data
  where study_id = upper(trialID);
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Count for tm_lz.lt_rwg_analysis_data= ' || vWZcount,0,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  insert into tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW(fold_change_ratio, raw_pvalue, adjusted_pvalue, 
  bio_assay_analysis_id, feature_group_name, bio_experiment_id, bio_assay_platform_id, 
  etl_id, preferred_pvalue,lsmean1, lsmean2, bio_assay_feature_group_id)
  select rad.fold_change, rad.raw_pvalue,rad.adjusted_pvalue, rad.bio_assay_analysis_id, rad.probeset, vExpID, vPlatformID,
  rad.study_id || ':RWG',rad.pvalue, rad.min_lsmean, rad.max_lsmean, bafg.bio_assay_feature_group_id
    from tm_wz.rwg_analysis_data rad 
  left outer join  BIOMART.bio_assay_feature_group bafg on (bafg.feature_group_name = rad.probeset)
  where rad.study_id = upper(trialID);		-- 20121212	JEA


  get diagnostics rowCt := ROW_COUNT;
  
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into BIO_ASSAY_ANALYSIS_DATA_NEW',rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  
  
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

  get diagnostics rowCt := ROW_COUNT;
    
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into tm_wz.tmp_assay_analysis_metrics ',rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  


update tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW
set tea_normalized_pvalue = (select biomart.tea_npv_precompute(fold_change_ratio, m.fc_mean, fc_stddev))
from tm_wz.tmp_assay_analysis_metrics m
where tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW.bio_assay_analysis_id = m.bio_assay_analysis_id 
and fold_change_ratio is not null;

get diagnostics rowCt := ROW_COUNT;
  
/*
merge into tm_wz.BIO_ASSAY_ANALYSIS_DATA_NEW d
using tm_wz.tmp_assay_analysis_metrics m 
   on (d.bio_assay_analysis_id = m.bio_assay_analysis_id and d.fold_change_ratio is not null)
when matched then
   update set d.tea_normalized_pvalue = TEA_NPV_PRECOMPUTE(d.fold_change_ratio, m.fc_mean, m.fc_stddev);
*/
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Update TEA records in tm_lz.BIO_ASSAY_ANALYSIS_DATA_NEW',rowCt,Stepct,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
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

      get diagnostics rowCt := ROW_COUNT;
                
      select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into biomart.bio_assay_analysis_data for analysis:  ' || cDeleteRow.bio_assay_analysis_id,rowCt,Stepct,'Done') into rtnCd;
      --select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into biomart.bio_assay_analysis_data',rowCt,Stepct,'Done') into rtnCd;
	  stepCt := stepCt + 1;	
      
      
  end loop;



delete from biomart.bio_data_omic_marker 
where bio_data_id in 
(select baad.bio_asy_analysis_data_id 
from biomart.bio_assay_analysis_data baad
where baad.bio_experiment_id= vExpID);

get diagnostics rowCt := ROW_COUNT;
select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete records from biomart.bio_data_omic_marker',rowCt,Stepct,'Done') into rtnCd;
stepCt := stepCt + 1;	


insert into  biomart.bio_data_omic_marker (bio_data_id, bio_marker_id, data_table)
select baad.bio_asy_analysis_data_id, baada.bio_marker_id, 'BAAD'
from biomart.bio_assay_analysis_data baad, biomart.bio_assay_data_annotation baada
where baad.bio_assay_feature_group_id = baada.bio_assay_feature_group_id
and baad.bio_experiment_id= vExpID;

get diagnostics rowCt := ROW_COUNT;
select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records into  biomart.bio_data_omic_marker',rowCt,Stepct,'Done') into rtnCd;
stepCt := stepCt + 1;	



update biomart.bio_assay_analysis x
set data_count = 
(select count(*) 
from biomart.bio_assay_analysis_data baad
where baad.bio_assay_analysis_id = x.bio_assay_analysis_id)
where x.data_count is null;

get diagnostics rowCt := ROW_COUNT;
select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Update missing data counts in biomart.bio_assay_analysis',rowCt,Stepct,'Done') into rtnCd;
stepCt := stepCt + 1;	


  

  get diagnostics rowCt := ROW_COUNT;
  
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Procedure Complete',rowCt,stepCt,'Done') into rtnCd;
  Stepct := Stepct + 1;	
  
  

	return 1;
  
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.rwg_load_analysis_data(character varying, numeric, character varying) SET search_path=tm_cz, i2b2demodata, i2b2metadata, pg_temp;

ALTER FUNCTION tm_cz.rwg_load_analysis_data(character varying, numeric, character varying)
  OWNER TO tm_cz;
