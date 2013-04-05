set define off;
  CREATE OR REPLACE PROCEDURE "I2B2_ADD_PROBESET_FEATURE_GRP" 
(
i_platform	 varchar2
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
	
	gplId	varchar2(100);
	pCount	int;
	
	no_platform exception;

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
	cz_write_audit(jobId,databaseName,procedureName,'Starting '||procedureName,0,stepCt,'Done');

	--	check platform exists in tm_cz.probeset_deapp
	
	select count(*) into pCount
	from tm_cz.probeset_deapp
	where platform = i_platform;
	
	if pcount = 0 then
		raise no_platform;
	end if;
	
	--	insert probesets into biomart.bio_assay_feature_group
	
	insert into biomart.bio_assay_feature_group
	(feature_group_name
	,feature_group_type)
	select distinct t.probeset, 'PROBESET'
	from tm_cz.probeset_deapp t
	where t.platform = i_platform
	  and not exists
		 (select 1 from biomart.bio_assay_feature_group x
		  where t.probeset = x.feature_group_name);
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert probesets into biomart.bio_assay_feature_group',SQL%ROWCOUNT,stepCt,'Done');
		  
	--	insert probesets into biomart.bio_assay_data_annotation
	
	insert into biomart.bio_assay_data_annotation
	(bio_assay_feature_group_id
	,bio_marker_id)
	select distinct fg.bio_assay_feature_group_id
		  ,coalesce(bgs.bio_marker_id,bgi.bio_marker_id)
	from tm_cz.probeset_deapp t
		,deapp.de_mrna_annotation ma
		,biomart.bio_assay_feature_group fg
		,biomart.bio_marker bgs
		,biomart.bio_marker bgi
	where t.platform = i_platform
	  and t.probeset_id = ma.probeset_id
	  and (ma.gene_symbol is not null or ma.gene_id is not null)
	  and t.probeset = fg.feature_group_name
	  and ma.gene_symbol = bgs.bio_marker_name(+)
	  and upper(coalesce(t.organism,'Homo sapiens')) = upper(bgs.organism)
	  and to_char(ma.gene_id) = bgi.primary_external_id(+)
	  and upper(coalesce(t.organism,'Homo sapiens')) = upper(bgi.organism)
	  and coalesce(bgs.bio_marker_id,bgi.bio_marker_id,-1) > 0
	  and not exists
		 (select 1 from biomart.bio_assay_data_annotation x
		  where fg.bio_assay_feature_group_id = x.bio_assay_feature_group_id
		    and coalesce(bgs.bio_marker_id,bgi.bio_marker_id) = x.bio_marker_id);
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Link feature_group to bio_marker in biomart.bio_assay_data_annotation',SQL%ROWCOUNT,stepCt,'Done');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End '|| procedureName,0,stepCt,'Done');
	
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

