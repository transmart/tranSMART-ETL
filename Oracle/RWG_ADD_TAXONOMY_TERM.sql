set define off;

  CREATE OR REPLACE PROCEDURE "RWG_ADD_TAXONOMY_TERM" 
(
  New_Term_in Varchar2,
  parent_term_in Varchar2,
  category_term_in varchar2,
  currentJobID NUMBER := null
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

	Parent_Id Int;
  New_Term_in_Id Int;
  keyword_id int;
	Lcount Int; 
  Ncount Int;
  Existing_Term Exception;

  
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
  
  

/*
0. Check if term exists in Search_Keyword_term
1. Insert term into Searchapp.search_keyword
2. Insert term into Searchapp.Search_Keyword_term
3. Find parent
4. Insert new term into Searchapp.Search_Taxonomy
5. Find id of new term
6. Insert relationship into searchapp.search_taxonomy_rels

*/



-- Get the data category using the parent term
/*
    Select distinct(data_category)
    into category_term_in
  From Searchapp.Search_Keyword Where Upper(Keyword) 
  like upper(parent_term_in) or upper(display_data_category) like upper(parent_term_in);
*/

-- check if the new term exists (use the keyword AND the category, as the same
-- term name may be used in more than 1 category
Select Count(*) 
into Ncount
  From  Searchapp.Search_Keyword 
  Where Upper(Keyword) = Upper(New_Term_in)
  and upper(data_category) like upper(category_term_in);


--If(Ncount>0) Then
 -- RAISE Existing_Term;
--END IF;



-- Insert taxonomy term into searchapp.search_keyword

	if Ncount = 0 then
	  Insert Into Searchapp.Search_Keyword (Data_Category, Keyword, Unique_Id, Source_Code, Display_Data_Category)
	  Select distinct(data_category), New_Term_in, 'RWG:'|| data_category || ':' || New_Term_in,
	  'RWG_ADD_TAXONOMY_TERM', Display_Data_Category
	  From Searchapp.Search_Keyword 
	  Where  upper(display_data_category) like upper(category_term_in);
	/*
	  Insert Into Searchapp.Search_Keyword (Data_Category, Keyword, Unique_Id, Source_Code, Display_Data_Category)
	  Select category_term_in, New_Term_in, 'RWG:'|| category_term_in || ':' || New_Term_in, 'RWG_ADD_TAXONOMY_TERM', category_term_in from dual;
	*/

	  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term added to Searchapp.Search_Keyword',Sql%Rowcount,Stepct,'Done');
	  Stepct := Stepct + 1;
	end if;
  
    -- Get the ID of the new term in Search_Keyword
  Select Search_Keyword_Id  Into Keyword_Id 
  From  Searchapp.Search_Keyword Where Upper(Keyword) = Upper(New_Term_in)
    and upper(data_category) like upper(category_term_in);
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'New search keyword ID stored in Keyword_Id',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	
    
  
  -- Insert the new term into Searchapp.Search_Keyword_Term 
  Insert Into Searchapp.Search_Keyword_Term 
      (Keyword_Term, Search_Keyword_Id, Rank, Term_Length)
  select New_Term_in, Keyword_Id, 1, Length(New_Term_in) from dual
  where not exists
	   (select 1 from searchapp.search_keyword_term x
	    where x.search_keyword_id = Keyword_Id);

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term added to Searchapp.Search_Keyword_Term',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	
  
  
  -- Get the ID of the parent term
Select Distinct(Term_Id)
 Into Parent_Id
  From Searchapp.Search_Taxonomy
Where upper(Term_Name) Like upper(parent_term_in);


-- Insert the new term into the taxonomy 
  insert into Searchapp.Search_Taxonomy (term_name, source_cd, import_date, search_keyword_id)
  Select New_Term_in, parent_term_in||':'||New_Term_in, Sysdate, Keyword_Id From dual
  where not exists
	   (select 1 from searchapp.search_taxonomy x
	    where x.search_keyword_id = Keyword_Id);
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term added to Searchapp.Search_Taxonomy',Sql%Rowcount,Stepct,'Done');
    Stepct := Stepct + 1;	

  -- Get the ID of the new term
Select Distinct(Term_Id)
 Into New_Term_in_Id
  From Searchapp.Search_Taxonomy
Where upper(Term_Name) Like upper(New_Term_in);


Insert Into Searchapp.Search_Taxonomy_Rels (Child_Id, Parent_Id)
select New_Term_in_Id, Parent_Id from dual
where not exists
	 (select 1 from searchapp.search_taxonomy_rels x
	  where x.child_id = New_Term_in_Id
	    and x.parent_id = Parent_id);
    Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term relationship added to Searchapp.Search_Taxonomy_Rels',Sql%Rowcount,Stepct,'Done');
    Stepct := Stepct + 1;	

 
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

