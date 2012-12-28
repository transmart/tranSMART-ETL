set define off;

  CREATE OR REPLACE PROCEDURE "RWG_LOAD_HEAT_MAP_RESULTS" 
(
  In_Study_Id In Varchar2
  ,currentJobID NUMBER := null
) as
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
  sqlText varchar(500);
  
  partExists integer(1);
   
   
   cursor cInsert is 
    Select Distinct Decode(B1.Study_Id, 'C0524T03_RWG', 'C0524T03', B1.Study_Id) Study_Id, B1.Bio_Assay_Analysis_Id, Cohort_Id
    from biomart.bio_analysis_attribute b1, biomart.bio_analysis_cohort_xref b2
    Where B1.Bio_Assay_Analysis_Id = B2.Bio_Assay_Analysis_Id
    and upper(b1.study_id)=upper(in_study_id);


   Cursor Cdelete Is 
    Select Distinct Bio_Assay_Analysis_Id
    From biomart.Heat_Map_Results
    where upper(trial_name) = upper(in_study_id);    

    cInsertRow cInsert%rowtype;
    cDeleteRow cDelete%rowtype;
    i integer;
begin

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
    	
  Stepct := 0;
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Start Procedure',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	


	  
--	check if partition exists

	select count(*) 
	into partExists
	from all_tab_partitions
	where table_name = 'HEAT_MAP_RESULTS'
	  and table_owner = 'BIOMART'
	  and partition_name = upper(in_study_id);

		if partExists = 0 then
--	needed to add partition to table
			sqlText := 'alter table BIOMART.HEAT_MAP_RESULTS  add PARTITION "' || upper(in_study_id) || '"  VALUES (' || '''' || upper(in_study_id) || '''' || ') ' ||
					'PCTFREE 0 PCTUSED 40 INITRANS 1 MAXTRANS 255  NOLOGGING ' ||
				   'STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 ' ||
				   'PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) ';

			execute immediate(sqlText);
			cz_write_audit(jobId,databaseName,procedureName,'Adding partition to BIOMART.HEAT_MAP_RESULTS ',0,stepCt,'Done');
			stepCt := stepCt + 1;

		else 
    --truncate table
			sqlText := 'alter table BIOMART.HEAT_MAP_RESULTS truncate partition ' || upper(in_study_id);
			execute immediate(sqlText);
      
      cz_write_audit(jobId,databaseName,procedureName,'Truncate partition in BIOMART.HEAT_MAP_RESULTS',0,stepCt,'Done');
			stepCt := stepCt + 1;
      
		end if;

/*


  For Cdeleterow In Cdelete Loop
    Delete From Biomart.Heat_Map_Results
    where upper(trial_name)=upper(in_study_id)
    and bio_assay_analysis_id = cDeleteRow.bio_assay_analysis_id;
    
      dbms_output.put_line('Delete count for ' || cDeleteRow.bio_assay_analysis_id || '=' || SQL%ROWCOUNT);
      
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete records for analysis:  ' || cDeleteRow.bio_assay_analysis_id,Sql%Rowcount,Stepct,'Done');
    stepCt := stepCt + 1;	
      
    commit;
  end loop;
*/

	--	changed to use sql instead of view, view pulled back all studies   20121203 JEA

  For Cinsertrow In Cinsert Loop
	  insert into biomart.heat_map_results
	  (subject_id
	  ,log_intensity
	  ,cohort_id
	  ,probe_id
	  ,bio_assay_feature_group_id
	  ,fold_change_ratio
	  ,tea_normalized_pvalue
	  ,bio_marker_name
	  ,bio_marker_id
	  ,search_keyword_id
	  ,bio_assay_analysis_id
	  ,trial_name
	  ,significant
	  ,gene_id
	  ,assay_id
	  ,preferred_pvalue
	  )
      select replace(replace(pd.sourcesystem_cd,xref.study_id,''),':','') as subject_id
			,md.log_intensity
			,cex.cohort_id
			,dma.probeset
			,baad.bio_assay_feature_group_id
			,baad.Fold_Change_Ratio
			,baad.tea_normalized_pvalue as tea_normalized_pvalue
			,f.bio_marker_name
			,f.bio_marker_id
			,i.SEARCH_KEYWORD_ID
			,xref.bio_assay_analysis_id
			,xref.study_id
			,case when (Abs(baaD.Fold_Change_Ratio) > baa.Fold_Change_Cutoff Or baaD.Fold_Change_Ratio Is Null) 
							  AND  NVL(baad.preferred_pvalue, baad.tea_normalized_pvalue) < baa.pvalue_cutoff
							  AND ((baad.lsmean1 > baa.lsmean_cutoff OR baad.lsmean2 > baa.lsmean_cutoff) OR (baad.lsmean1 is null AND baad.lsmean2 is null))
				   THEN 1
				   Else 0
				   End Significant
			,f.Primary_External_Id
			,sm.assay_id
			,baad.preferred_pvalue
			from biomart.bio_analysis_cohort_xref xref
			inner join biomart.bio_cohort_exp_xref cex
				on xref.study_id = cex.study_id
				and xref.cohort_id = cex.cohort_id
			inner join deapp.de_subject_sample_mapping sm
				on xref.study_id = sm.trial_name
				and cex.exp_id = sm.assay_id
			inner join deapp.de_subject_microarray_data md
				on md.trial_source = xref.study_id || ':' || coalesce(sm.source_cd,'STD')
				and cex.exp_id = md.assay_id
			inner join tm_cz.probeset_deapp	dma			-- use tm_cz.probeset_deapp because there is only a single record for the probeset
				on md.probeset_id  = dma.probeset_id
			inner join i2b2demodata.patient_dimension pd
				on sm.patient_id = pd.patient_num
			inner join biomart.bio_assay_analysis_data baad
				on xref.bio_assay_analysis_id = baad.bio_assay_analysis_id
				and baad.feature_group_name = dma.probeset
			INNER JOIN biomart.bio_assay_data_annotation e on e.bio_assay_feature_group_id = baad.bio_assay_feature_group_id
			INNER JOIN biomart.bio_marker f on f.bio_marker_id = e.bio_marker_id
			inner join biomart.bio_assay_analysis baa
			  on xref.bio_assay_analysis_id = baa.bio_assay_analysis_id
			  Inner Join biomart.bio_marker_correl_mv h ON f.bio_marker_id = h.asso_bio_marker_id  AND h.correl_type in ('GENE', 'HOMOLOGENE_GENE', 'PROTEIN TO GENE')
			Inner Join searchapp.search_keyword i ON f.bio_marker_id = i.bio_data_id
			where xref.bio_assay_analysis_id = cInsertRow.bio_assay_analysis_id
			and cex.cohort_id = cInsertRow.cohort_id;

     -- dbms_output.put_line('Insert count for ' || Cinsertrow.bio_assay_analysis_id || '=' || SQL%ROWCOUNT);
      
      
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert count for analysis:  ' || Cinsertrow.bio_assay_analysis_id || ' cohort: ' || cInsertRow.cohort_id,Sql%Rowcount,Stepct,'Done');
    stepCt := stepCt + 1;	
    commit;
    
    update BIOMART.bio_assay_analysis baa
    set baa.analysis_update_date = sysdate
    where baa.bio_assay_analysis_id = cInsertRow.bio_assay_analysis_id;
    
   Cz_Write_Audit(Jobid,Databasename,Procedurename,'Updated analysis_update_date for analysis:  ' || Cinsertrow.bio_assay_analysis_id,Sql%Rowcount,Stepct,'Done');
    stepCt := stepCt + 1;	
    
    
      commit;
    End Loop;
	
  cz_write_audit(jobId,databaseName,procedureName,'Procedure Complete',0,stepCt,'Done');
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
end;

