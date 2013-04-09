-- Function: tm_cz.rwg_import_from_ext(character varying, numeric)

-- DROP FUNCTION tm_cz.rwg_import_from_ext(character varying, numeric);

CREATE OR REPLACE FUNCTION tm_cz.rwg_import_from_ext(trialid character varying, currentjobid numeric DEFAULT (-1))
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

BEGIN

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := 'TM_CZ';
	procedureName := 'I2B2_LOAD_SECURITY';
	
	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select tm_cz.cz_start_audit (procedureName, databaseName) into jobId;
	END IF;


  
  delete from TM_WZ.Rwg_Analysis where upper(study_id) =upper(trialID);
  
  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Delete existing records from TM_WZ.Rwg_Analysis',rowCt,stepCt,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  
  
  
  --Insert Analysis
    INSERT  INTO TM_WZ.Rwg_Analysis
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
    now()
    From  TM_LZ.lt_RWG_ANALYSIS
    where upper(study_id)=upper(trialID);

  select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Insert records from TM_LZ.lt_RWG_ANALYSIS to TM_LZ.Rwg_Analysis',rowCt,stepCt,'Done') into rtnCd;
  stepCt := stepCt + 1;	
  
  
  
	--	update bio_assay_analysis_id for any existing analysis_id (20130111 JEA)
	
	update tm_wz.rwg_analysis t
	set bio_assay_analysis_id=(select b.bio_assay_analysis_id 
							   from biomart.bio_assay_analysis b
							   where b.etl_id = trialID || ':RWG'
							     and b.analysis_name = t.analysis_id)
	where t.study_id = trialID
	  and exists
		 (select 1 from biomart.bio_assay_analysis x
		  where x.etl_id = trialID || ':RWG'
		    and t.analysis_id = x.analysis_name);
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'Update bio_assay_analysis_id on existing rwg_analysis records',rowCt,stepCt,'Done') into rtnCd;
	stepCt := stepCt + 1;	
				
  
  stepCt := stepCt + 1;
	select tm_cz.cz_write_audit(jobId,databaseName,procedureName,'End RWG_IMPORT_FROM_EXT',rowCt,stepCt,'Done') into rtnCd;
	
	return 1;
  
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.rwg_import_from_ext(character varying, numeric) SET search_path=tm_cz, i2b2demodata, i2b2metadata, pg_temp;

ALTER FUNCTION tm_cz.rwg_import_from_ext(character varying, numeric)
  OWNER TO postgres;
