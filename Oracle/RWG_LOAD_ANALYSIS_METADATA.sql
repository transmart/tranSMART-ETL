set define off;
create or replace
PROCEDURE         "RWG_LOAD_ANALYSIS_METADATA" 
(
  trialID varchar2
  ,i_study_data_category	varchar2 := 'Study'
  ,i_study_category_display	varchar2
 ,currentJobID NUMBER := null
 ,rtn_code	OUT number
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

	Dcount Int;
	lcount int;
  analysisCount int;
  resultCount int;
  
   ANALYSIS_COUNT_MISMATCH EXCEPTION;
  
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
  
  
  /***                                                                                                     ***/
  /*   Before starting, ensure that the incoming analysis IDs match to the biomart.bio_assay_analysis name   */
  /*    If not, try to match using the short_desc. Update the analysis_name if this work; otherwise, quit    */
  /***                                                                                                     ***/
  
  /*NOTE: Due to a change in the curation/etl procedures, this step should no longer be needed. 
  The bio_assay_analysis_id is updated in TM_LZ.Rwg_Analysis at time of creation.
  A check is done, and if the IDs match, then this step is bypassed */
  
  
  -- get the count of the incoming analysis data
select count(*) into analysisCount 
from TM_LZ.Rwg_Analysis
where study_id =  Upper(trialID);

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Analysis count from TM_LZ.Rwg_Analysis =',analysisCount,Stepct,'Done');
  Stepct := Stepct + 1;	

--see how many of the analysees match by using the cohort analysis name
select count(*) into resultCount 
from TM_LZ.Rwg_Analysis analysis, Biomart.Bio_Assay_Analysis Baa
Where analysis.bio_assay_analysis_id= baa.bio_assay_analysis_id --bio_assay_analysis_id in 'TM_LZ.Rwg_Analysis analysis' should already exist
and upper(analysis.study_id) =  Upper(trialID);

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Join analysis.Cohorts to Baa.Analysis_Name, Analysis count =',resultCount,Stepct,'Done');
  Stepct := Stepct + 1;	
  
IF (analysisCount != resultCount)
THEN  
  RAISE ANALYSIS_COUNT_MISMATCH;
END IF;
  
  
  

-- check if the analysis counts match. If so, skip statement. If not, keep trying

/*
IF (analysisCount = resultCount)
THEN

    update TM_LZ.rwg_analysis analysis
    set analysis.bio_assay_analysis_id = (
        select baa.bio_assay_analysis_id
        from  Biomart.Bio_Assay_Analysis Baa
        Where trim(upper(analysis.Cohorts))= trim(upper(Baa.Analysis_Name))
        And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%'
        and analysis.study_id =  Upper(trialID)
        )
        where analysis.study_id =  Upper(trialID);
        
        Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update TM_LZ.rwg_analysis with BAA ID, matching on cohorts ID',Sql%Rowcount,Stepct,'Done');
        Stepct := Stepct + 1;	

ELSE

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Analysis count did not match. Enter ELSE statement',0,Stepct,'Done');
  Stepct := Stepct + 1;	

  --if the counts above do not match, try matching on short_desc
  select count(*) into resultCount 
  from  TM_LZ.Rwg_Analysis analysis, Biomart.Bio_Assay_Analysis Baa
  Where trim(upper(analysis.short_desc))= trim(upper(baa.short_description))
  and analysis.study_id =  Upper(trialID)
  And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%';
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Try matching on baa.short_description, Analysis count =',resultCount,Stepct,'Done');
  Stepct := Stepct + 1;	
  
  --check if the counts match
  IF (analysisCount =resultCount)
  THEN
  
      --if the counts do match, update the records in bio_assay_analysis to the new analysis name
      update BIOMART.bio_assay_analysis baa
      set baa.analysis_name = (
          select upper(trim(analysis.cohorts)) from  TM_LZ.Rwg_Analysis analysis
          Where trim(upper(analysis.short_desc))= trim(upper(baa.short_description))
          and analysis.study_id =  Upper(trialID)
      ) where Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%';
      
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update BIOMART.bio_assay_analysis with new analysis name',Sql%Rowcount,Stepct,'Done');
    Stepct := Stepct + 1;	
    
    update TM_LZ.rwg_analysis analysis
    set analysis.bio_assay_analysis_id = (
        select baa.bio_assay_analysis_id
        from  Biomart.Bio_Assay_Analysis Baa
        Where trim(upper(analysis.short_desc))= trim(upper(baa.short_description))
        And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%'
        and analysis.study_id =  Upper(trialID)
        )
        where analysis.study_id =  Upper(trialID);
        
        Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update TM_LZ.rwg_analysis with BAA ID, matching on short desc',Sql%Rowcount,Stepct,'Done');
        Stepct := Stepct + 1;	
    
  
  ELSE
      --If matching the short_desc did not work, try again using the long_desc
        select count(*) into resultCount 
        from  TM_LZ.Rwg_Analysis analysis, Biomart.Bio_Assay_Analysis Baa
        Where trim(upper(analysis.long_desc))= trim(upper(baa.long_description))
        and analysis.study_id =  Upper(trialID)
        And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%';
        
        Cz_Write_Audit(Jobid,Databasename,Procedurename,'Try matching on baa.long_description, Analysis count =',resultCount,Stepct,'Done');
        Stepct := Stepct + 1;	
        
        --check if the counts match
        IF (analysisCount =resultCount)
        THEN
        
            --if the counts do match, update the records in bio_assay_analysis to the new analysis name
            update BIOMART.bio_assay_analysis baa
            set baa.analysis_name = (
                select upper(trim(analysis.cohorts)) from  TM_LZ.Rwg_Analysis analysis
                Where trim(upper(analysis.long_desc))= trim(upper(baa.long_description))
                and analysis.study_id =  Upper(trialID)
            ) where Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%';
            
          Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update BIOMART.bio_assay_analysis with new analysis name',Sql%Rowcount,Stepct,'Done');
          Stepct := Stepct + 1;	
          
                
          update TM_LZ.rwg_analysis analysis
          set analysis.bio_assay_analysis_id = (
              select baa.bio_assay_analysis_id
              from  Biomart.Bio_Assay_Analysis Baa
              Where trim(upper(analysis.long_desc))= trim(upper(baa.long_description))
              And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%'
              and analysis.study_id =  Upper(trialID)
              )
          where analysis.study_id =  Upper(trialID);
          
          Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update TM_LZ.rwg_analysis with BAA ID, matching on long desc',Sql%Rowcount,Stepct,'Done');
          Stepct := Stepct + 1;	

        
        ELSE
          --if these counts do not match, then throw exception
          RAISE ANALYSIS_COUNT_MISMATCH;
        END IF;
    END IF;
END IF;
      */


  
  
  delete from Biomart.Bio_Analysis_Cohort_Xref  where upper(study_id) =upper(trialID);
  
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from Biomart.Bio_Analysis_Cohort_Xref',Sql%Rowcount,Stepct,'Done');
  stepCt := stepCt + 1;	
  
  
  Delete  From Biomart.Bio_Analysis_Attribute_Lineage Baal
Where Baal.Bio_Analysis_Attribute_Id In
 (Select Distinct(Baa.Bio_Analysis_Attribute_Id)
 from Biomart.Bio_Analysis_Attribute baa  where upper(study_id) = upper(trialID));
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from Biomart.Bio_Analysis_Attribute_Lineage',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	
  
  delete from Biomart.Bio_Analysis_Attribute  where upper(study_id) =upper(trialID);
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from Biomart.Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	

/** Delete study from biomart.bio_assay_cohort table **/
delete from Biomart.Bio_Assay_Cohort where upper(study_id) = upper(trialID);

Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing records from Biomart.Bio_Assay_Cohort',Sql%Rowcount,Stepct,'Done');
Stepct := Stepct + 1;	



/** Populate biomart.bio_assay_cohort table **/
Insert Into Biomart.Bio_Assay_Cohort (Study_Id, Cohort_Id, Disease, Sample_Type, Treatment,
Organism, Pathology, Cohort_Title, Short_Desc, Long_Desc, Import_Date)
Select Study_Id, Cohort_Id, Disease, Sample_Type, Treatment,
Organism, Pathology, Cohort_Title, Short_Desc, Long_Desc, Sysdate From TM_LZ.Rwg_Cohorts
Where Upper(Study_Id) = Upper(trialID);

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert into Biomart.Bio_Assay_Cohort',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	


 Select NVL(Max(Length(Regexp_Replace(analysis.Cohorts,'[^;]'))),0)+1
 into dcount
 FROM TM_LZ.Rwg_Analysis analysis;

  for lcount in 1 .. dcount
        Loop	
        
      Stepct := Stepct + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Starting Bio_Analysis_Cohort_Xref LOOP, pass: ',lcount,stepCt,'Done');


    Insert Into  Biomart.Bio_Analysis_Cohort_Xref (Study_Id, Analysis_Cd, Cohort_Id, Bio_Assay_Analysis_Id)   
      Select upper(analysis.Study_Id), analysis.Cohorts
      ,trim(Parse_Nth_Value(analysis.Cohorts,lcount,';')) as cohort, baa.bio_assay_analysis_id
      From TM_LZ.Rwg_Analysis analysis, Biomart.Bio_Assay_Analysis Baa
      Where analysis.bio_assay_analysis_id= Baa.bio_assay_analysis_id
      And Upper(Baa.Etl_Id) Like '%' || Upper(Trialid) || '%'
      And Upper(analysis.Study_Id) Like '%' || Upper(Trialid) || '%'
      And Trim(Parse_Nth_Value(analysis.Cohorts,lcount,';')) Is Not Null;
      
      
    
      Stepct := Stepct + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Insert COHORTS into  into Biomart.Bio_Analysis_Cohort_Xref',SQL%ROWCOUNT,stepCt,'Done');
      commit;					
          
  end loop;
  
    
  
  
  /*************************************/
  /** POPULATE Bio_Analysis_Attribute **/
  /*************************************/
  
	--	delete study from cz_rwg_invalid_terms 20121220 JEA
	
	delete cz_rwg_invalid_terms
	where upper(study_id) = upper(trialID);
	commit;
    Stepct := Stepct + 1;
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Delete existing data from cz_rwg_invalid_terms',Sql%Rowcount,Stepct,'Done');
    commit;  
	
	--	insert study as search_term term, sp will check if already exists
	
	rwg_add_search_term(upper(trialID),i_study_data_category,i_study_category_display,jobId);

-- sample_type: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(Cohort.Study_Id), upper('sample_type'), cohort.sample_type
From TM_LZ.Rwg_Cohorts Cohort
Where upper(Cohort.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(Cohort.sample_type) = Upper(Tax.Term_Name));

-- sample_type: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Cohort.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('sample_type:' ||Cohort.sample_type) 
From  TM_LZ.Rwg_Cohorts Cohort, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Cohort.Cohort_Id) = upper(Xref.Cohort_Id)
And upper(Xref.Study_Id) = upper(Cohort.Study_Id)
And Upper(Cohort.Sample_Type) = Upper(Tax.Term_Name)
And Cohort.Study_Id =upper(trialID); 

      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert SAMPLE_TYPE into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	

-- disease: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct  upper(Cohort.Study_Id), 'disease', cohort.disease
From TM_LZ.Rwg_Cohorts Cohort
Where upper(Cohort.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(Cohort.disease) = Upper(Tax.Term_Name));

-- disease: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Cohort.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('disease:' ||Cohort.disease) 
From  TM_LZ.Rwg_Cohorts Cohort, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Cohort.Cohort_Id) = upper(Xref.Cohort_Id)
And upper(Xref.Study_Id) = upper(Cohort.Study_Id)
And Upper(Cohort.Disease) = Upper(Tax.Term_Name)
and upper(cohort.study_id) =upper(trialID); 


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert disease into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	

-- pathology: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(Cohort.Study_Id), 'pathology', cohort.pathology
From TM_LZ.Rwg_Cohorts Cohort
Where upper(Cohort.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(Cohort.pathology) = Upper(Tax.Term_Name));

-- pathology: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Cohort.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('pathology:' ||Cohort.pathology) 
From  TM_LZ.Rwg_Cohorts Cohort, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Cohort.Cohort_Id) = upper(Xref.Cohort_Id)
And Upper(Xref.Study_Id) = Upper(Cohort.Study_Id)
And Upper(Cohort.Pathology) = Upper(Tax.Term_Name)
and upper(cohort.study_id) =upper(trialID); 


Stepct := Stepct + 1;
Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert pathology into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
commit;	
      
      
-- LOOP FOR TREATMENT

 Select NVL(Max(Length(Regexp_Replace(Cohort.Treatment,'[^;]'))),0)+1
 Into Dcount
 From TM_LZ.Rwg_Cohorts Cohort
 Where upper(Cohort.Study_Id)=upper(trialID);

  for lcount in 1 .. dcount
        Loop	
        
      Stepct := Stepct + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Starting COHORT TREATMENT LOOP, pass: ',lcount,stepCt,'Done');

    -- treatment: check for any records that do not have a match in the taxonomy
    Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
     Select distinct upper(Cohort.Study_Id), 'treatment', trim(Parse_Nth_Value(cohort.treatment,lcount,';'))
    From TM_LZ.Rwg_Cohorts Cohort
    Where upper(Cohort.Study_Id)=upper(trialID)
    and Not Exists 
    (Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
    Where Upper(trim(Parse_Nth_Value(cohort.treatment,lcount,';'))) = Upper(Tax.Term_Name))
  And Trim(Parse_Nth_Value(cohort.treatment,lcount,';')) Is Not Null;


    -- treatment: insert terms into attribute table
    Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
    Select Distinct upper(Cohort.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('treatment:' ||trim(Parse_Nth_Value(cohort.treatment,lcount,';'))) 
    From  TM_LZ.Rwg_Cohorts Cohort, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
    Where upper(Cohort.Cohort_Id) = upper(Xref.Cohort_Id)
    And upper(Xref.Study_Id) = upper(Cohort.Study_Id)
    And Upper(Trim(Parse_Nth_Value(Cohort.Treatment,Lcount,';'))) = Upper(Tax.Term_Name)
    and upper(cohort.study_id) =upper(trialID); 


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert treatment into Bio_Analysis_Attribute (LOOP)',Sql%Rowcount,Stepct,'Done');
      commit;	
          
          
  End Loop;
    
      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'END TREATMENT LOOP',Sql%Rowcount,Stepct,'Done');
      Commit;	
-- END TREATMENT LOOP






-- organism: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(Cohort.Study_Id), 'organism', Cohort.organism
From TM_LZ.Rwg_Cohorts Cohort
Where upper(Cohort.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(Cohort.organism) = Upper(Tax.Term_Name));

-- organism: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Cohort.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('organism:' ||Cohort.Organism) 
From  TM_LZ.Rwg_Cohorts Cohort, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where Upper(Cohort.Cohort_Id) = Upper(Xref.Cohort_Id)
And upper(Xref.Study_Id) = upper(Cohort.Study_Id)
And Upper(Cohort.Organism) = Upper(Tax.Term_Name)
and upper(cohort.study_id) =upper(trialID); 


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert organism into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      Commit;	
      

-- data_type: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct  upper(analysis.Study_Id), 'data_type', analysis.data_type
From TM_LZ.Rwg_Analysis analysis
Where upper(analysis.Study_Id)=upper(trialID)
and  Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(analysis.data_type) = Upper(Tax.Term_Name));

-- data_type: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select upper(analysis.Study_Id), Baa.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('data_type:' ||analysis.Data_Type)
From TM_LZ.Rwg_Analysis analysis, Searchapp.Search_Taxonomy Tax, Biomart.Bio_Assay_Analysis Baa
Where Upper(analysis.Data_Type) = Upper(Tax.Term_Name)
And  analysis.bio_assay_analysis_id= Baa.bio_assay_analysis_id
And Upper(Baa.Etl_Id) Like '%' || Upper(analysis.Study_Id) || '%'
And upper(analysis.Study_Id)=upper(trialID);


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert data_type into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	

-- platform: check for any records that do not have a match in the taxonomy
insert into Cz_Rwg_Invalid_Terms (study_id, category_name, term_name)
Select distinct analysis.Study_Id, 'platform', analysis.platform
From TM_LZ.Rwg_Analysis analysis
Where upper(analysis.Study_Id)=upper(trialID)
and  Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(analysis.platform) = Upper(Tax.Term_Name));

-- platform: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select upper(analysis.Study_Id), Baa.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('platform:' ||analysis.Platform)
From TM_LZ.Rwg_Analysis analysis, Searchapp.Search_Taxonomy Tax, Biomart.Bio_Assay_Analysis Baa
Where Upper(analysis.Platform) = Upper(Tax.Term_Name)
And  analysis.bio_assay_analysis_id= Baa.bio_assay_analysis_id
And Upper(Baa.Etl_Id) Like '%' || Upper(analysis.Study_Id) || '%'
And upper(analysis.Study_Id)=upper(trialID);


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert platform into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	

-- LOOP FOR ANALYSIS TYPE

 Select NVL(Max(Length(Regexp_Replace(analysis.Analysis_Type,'[^;]'))),0)+1
 Into Dcount
 From TM_LZ.Rwg_Analysis analysis
 where upper(analysis.Study_Id)=upper(trialID);

  For Lcount In 1 .. Dcount
        Loop	 
        
      Stepct := Stepct + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Starting ANALYSIS_TYPE LOOP, pass: ',lcount,stepCt,'Done');

  
    
    -- Analysis_Type: check for any records that do not have a match in the taxonomy
    Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
    Select distinct upper(analysis.Study_Id), 'Analysis_Type', trim(Parse_Nth_Value(analysis.Analysis_Type,lcount,';'))
    From TM_LZ.Rwg_Analysis analysis
    Where upper(analysis.Study_Id)=upper(trialID)
    and  Not Exists 
    (Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
    Where Upper(Trim(Parse_Nth_Value(analysis.Analysis_Type,Lcount,';'))) = Upper(Tax.Term_Name))
     And Trim(Parse_Nth_Value(analysis.Analysis_Type,Lcount,';')) Is Not Null;
    
    -- Analysis_Type: insert terms into attribute table
    Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
    Select upper(analysis.study_id), baa.bio_assay_analysis_id, tax.term_id, upper('ANALYSIS_TYPE:' ||trim(Parse_Nth_Value(analysis.Analysis_Type,lcount,';')))
    From TM_LZ.Rwg_Analysis analysis, Searchapp.Search_Taxonomy Tax, Biomart.Bio_Assay_Analysis Baa
    Where upper(Trim(Parse_Nth_Value(analysis.Analysis_Type,Lcount,';'))) = Upper(Tax.Term_Name)
    And  analysis.bio_assay_analysis_id= Baa.bio_assay_analysis_id
    And Upper(Baa.Etl_Id) Like '%' || Upper(analysis.Study_Id) || '%'
    And upper(analysis.Study_Id)=Upper(Trialid)
    And Trim(Parse_Nth_Value(analysis.Analysis_Type,Lcount,';')) Is Not Null;
    

      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'LOOP: Insert ANALYSIS_TYPE into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	
      
  End Loop;
    
      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'END ANALYSIS_TYPE LOOP',Sql%Rowcount,Stepct,'Done');
      Commit;	
-- END ANALYSIS TYPE

-- search_area: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(ext.Study_Id), 'search_area', ext.search_area
From TM_LZ.clinical_trial_metadata_ext Ext
Where upper(ext.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(ext.search_area) = Upper(Tax.Term_Name));

-- search_area: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Ext.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('search_area:' ||Ext.search_area) 
From  TM_LZ.Clinical_Trial_Metadata_Ext Ext, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Xref.Study_Id) = upper(ext.Study_Id)
And Upper(Ext.Search_Area) = Upper(Tax.Term_Name)
and upper(ext.study_id) =upper(trialID); 


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert search_area into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      Commit;	
      
      
      
      

-- data source: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(ext.Study_Id), 'DATA_SOURCE', ext.data_source
From TM_LZ.Clinical_Trial_Metadata_Ext Ext
Where upper(ext.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(ext.data_source) = Upper(Tax.Term_Name));

-- data source: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Ext.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('DATA_SOURCE:' ||Ext.data_source) 
From  TM_LZ.Clinical_Trial_Metadata_Ext Ext, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Xref.Study_Id) = upper(ext.Study_Id)
And Upper(Ext.data_source) = Upper(Tax.Term_Name)
and upper(ext.study_id) =upper(trialID); 

Stepct := Stepct + 1;
Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert DATA_SOURCE into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
Commit;	
      
    

-- study_design: check for any records that do not have a match in the taxonomy
Insert Into Cz_Rwg_Invalid_Terms (Study_Id, Category_Name, Term_Name)
Select distinct upper(ext.Study_Id), 'study_design', ext.study_design
From TM_LZ.Clinical_Trial_Metadata_Ext Ext
Where upper(ext.Study_Id)=upper(trialID)
and Not Exists 
(Select Upper(Tax.Term_Name) From Searchapp.Search_Taxonomy Tax
Where Upper(ext.experimental_design) = Upper(Tax.Term_Name));

  -- study_design: insert terms into attribute table
Insert Into Biomart.Bio_Analysis_Attribute (Study_Id, Bio_Assay_Analysis_Id, Term_Id, Source_Cd)
Select Distinct upper(Ext.Study_Id), Xref.Bio_Assay_Analysis_Id, Tax.Term_Id, Upper('study_design:' ||Ext.experimental_design) 
From  TM_LZ.Clinical_Trial_Metadata_Ext Ext, Biomart.Bio_Analysis_Cohort_Xref Xref, Searchapp.Search_Taxonomy Tax
Where upper(Xref.Study_Id) = upper(ext.Study_Id)
And Upper(
        decode(Ext.experimental_design,'Clinical','Clinical Trial',
                                ext.experimental_design)
        )= Upper(Tax.Term_Name)
and upper(ext.study_id) =upper(trialID); 


      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert study_design into Bio_Analysis_Attribute',Sql%Rowcount,Stepct,'Done');
      commit;	

	--	populate biomart.bio_analysis_attribute_lineage in one shot
	
	insert into biomart.bio_analysis_attribute_lineage
	(bio_analysis_attribute_id
	,ancestor_term_id
	,ancestor_search_keyword_id)
	select baa.bio_analysis_attribute_id
		  ,baa.term_id
		  ,st.search_keyword_id
	from biomart.bio_analysis_attribute baa
		,searchapp.search_taxonomy st
	where upper(baa.study_id) = upper(trialID)
	  and baa.term_id = st.term_id;

	Stepct := Stepct + 1;
	cz_Write_Audit(Jobid,Databasename,Procedurename,'Insert attribute links into bio_analysis_attribute_lineage',Sql%Rowcount,Stepct,'Done');
	commit;	
	  
	  
/* END populate */

/*Update the 'analysis_update_date' in bio_assay_analysis (this date is used by solr for incremental updates*/
      update BIOMART.bio_assay_analysis baa
      set baa.ANALYSIS_UPDATE_DATE = sysdate
      where upper(baa.etl_id) like upper(trialID||'%');

      Stepct := Stepct + 1;
      Cz_Write_Audit(Jobid,Databasename,Procedurename,'Update ANALYSIS_UPDATE_DATE with sysdate',Sql%Rowcount,Stepct,'Done');
      commit;	


  cz_write_audit(jobId,databaseName,procedureName,'End Procedure',SQL%ROWCOUNT,stepCt,'Done');

     ---Cleanup OVERALL JOB if this proc is being run standalone    
  IF newJobFlag = 1
  THEN
    cz_end_audit (jobID, 'SUCCESS');
  END IF;
  
    EXCEPTION
	when ANALYSIS_COUNT_MISMATCH then
		Stepct := Stepct + 1;
		Cz_Write_Audit(Jobid,Databasename,Procedurename,'Check for analysis in rwg_analysis not in biomart.bio_assay_analysis',Sql%Rowcount,Stepct,'Done');
    --Handle errors.
    cz_error_handler (jobID, procedureName);
    --End Proc
    cz_end_audit (jobID, 'FAIL');
	rtn_code := 16;		
		
  WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler (jobID, procedureName);
    --End Proc
    cz_end_audit (jobID, 'FAIL');
	rtn_code := 16;
  
END;