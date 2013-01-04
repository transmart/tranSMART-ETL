set define off;

  CREATE OR REPLACE PROCEDURE "RWG_ADD_SEARCH_TERM" 
(
  New_Term Varchar2,
  category_name Varchar2,
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
  new_Term_Id Int;
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
1. Check if term exists in Search_Keyword_term
2. Insert term into Searchapp.search_keyword
3. Insert term into Searchapp.Search_Keyword_term

*/

/*
-- check if the new term exists
Select Count(*) 
into Ncount
From Searchapp.Search_Keyword
where upper(Keyword) like upper(New_Term)
and upper(Display_Data_Category) like upper(category_name);

If(Ncount>0) Then
  RAISE Existing_Term;
END IF;
*/

-- Insert taxonomy term into searchapp.search_keyword
-- (searches Search_Keyword with the parent term to find the category to use)
  Insert Into Searchapp.Search_Keyword (Data_Category, Keyword, Unique_Id, Source_Code, Display_Data_Category)
  Select category_name, New_Term, 'RWG:'|| category_name || ':' || New_Term,
  'RWG_ADD_SEARCH_TERM', category_name
  From dual
  where not exists
	    (select 1 from searchapp.search_keyword x
		 where upper(x.data_category) = upper(category_name)
		   and upper(x.keyword) = upper(New_Term));

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term added to Searchapp.Search_Keyword',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	
  
    -- Get the ID of the new term in Search_Keyword
  Select Search_Keyword_Id  Into Keyword_Id 
  From  Searchapp.Search_Keyword Where Upper(Keyword) = Upper(New_Term)
  and upper(data_category) = upper(category_name);
  Cz_Write_Audit(Jobid,Databasename,Procedurename,'New search keyword ID stored in Keyword_Id',Sql%Rowcount,Stepct,'Done');
  Stepct := Stepct + 1;	
    
  
  -- Insert the new term into Searchapp.Search_Keyword_Term 
  Insert Into Searchapp.Search_Keyword_Term 
      (Keyword_Term, Search_Keyword_Id, Rank, Term_Length)
  select New_Term, Keyword_Id, 1, Length(New_Term) from dual
  where not exists
	    (select 1 from searchapp.search_keyword_term x
		 where upper(x.keyword_term) = upper(New_Term)
		   and x.search_keyword_id = Keyword_Id);

  Cz_Write_Audit(Jobid,Databasename,Procedurename,'Term added to Searchapp.Search_Keyword_Term',Sql%Rowcount,Stepct,'Done');
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

/
