-- Function: tm_cz.i2b2_load_security_data(numeric)

-- DROP FUNCTION tm_cz.i2b2_load_security_data(currentJobID	numeric default -1);

CREATE OR REPLACE FUNCTION tm_cz.i2b2_load_security_data(
 currentJobID 	numeric default 0
)
  RETURNS integer AS
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

	rtnCd integer;
  --Audit variables
  newJobFlag int4;
  databaseName VARCHAR(100);
  procedureName VARCHAR(100);
  jobID numeric(18,0);
  stepCt numeric(18,0);
  rowCt		numeric(18,0);
  
  secNodeExists		integer;
  etlDate			timestamp;
	errorNumber		character varying;
	errorMessage	character varying;


BEGIN

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)
  jobID := currentJobID;
  select clock_timestamp() into etlDate;

  databaseName := 'TM_CZ';
  procedureName := 'I2B2_LOAD_SECURITY_DATA';

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF(jobID IS NULL or jobID < 1)
  THEN
    newJobFlag := 1; -- True
    select tm_cz.czx_start_audit (procedureName, databaseName) into jobId;
  END IF;

  stepCt := 0;

  Execute 'truncate table I2B2METADATA.i2b2_secure';

  stepCt := stepCt + 1;
  select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Truncate I2B2METADATA i2b2_secure',0,stepCt,'Done')into rtnCd;

  begin
  insert into i2b2metadata.i2b2_secure(
    c_hlevel,
    c_fullname,
    c_name,
    c_synonym_cd,
    c_visualattributes,
    c_totalnum,
    c_basecode,
    c_metadataxml,
    c_facttablecolumn,
    c_tablename,
    c_columnname,
    c_columndatatype,
    c_operator,
    c_dimcode,
    c_comment,
    c_tooltip,
    update_date,
    download_date,
    import_date,
    sourcesystem_cd,
    valuetype_cd,
	i2b2_id,
	secure_obj_token)
  select
    b.c_hlevel,
    b.c_fullname,
    b.c_name,
    b.c_synonym_cd,
    b.c_visualattributes,
    b.c_totalnum,
    b.c_basecode,
    b.c_metadataxml,
    b.c_facttablecolumn,
    b.c_tablename,
    b.c_columnname,
    b.c_columndatatype,
    b.c_operator,
    b.c_dimcode,
    b.c_comment,
    b.c_tooltip,
    b.update_date,
    b.download_date,
    b.import_date,
    b.sourcesystem_cd,
    b.valuetype_cd,
	b.i2b2_id,
	coalesce(f.tval_char,'EXP:PUBLIC')
    from I2B2METADATA.I2B2 b
	left outer join (select distinct case when sourcesystem_cd like '%:%' then substr(sourcesystem_cd,1,tm_cz.instr(sourcesystem_cd,':')-1)
							   else sourcesystem_cd end as sourcesystem_cd
				,tval_char from i2b2demodata.observation_fact where concept_cd = 'SECURITY') f
	on b.sourcesystem_cd = f.sourcesystem_cd;
	get diagnostics rowCt := ROW_COUNT;
	exception
	when others then
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;
	end;
    stepCt := stepCt + 1;
    select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert security data into I2B2METADATA i2b2_secure',rowCt,stepCt,'Done')into rtnCd;
	
	--	check if SECURITY node exists in i2b2

	select count(*) into secNodeExists
	from i2b2metadata.i2b2
	where c_fullname = '\' || 'Public Studies' || '\' || 'SECURITY' || '\';

	if secNodeExists = 0 then
		begin
		insert into i2b2metadata.i2b2
		(c_hlevel
		,c_fullname
		,c_name
		,c_synonym_cd
		,c_visualattributes
		,c_totalnum
		,c_basecode
		,c_metadataxml
		,c_facttablecolumn
		,c_tablename
		,c_columnname
		,c_columndatatype
		,c_operator
		,c_dimcode
		,c_comment
		,c_tooltip
		,update_date
		,download_date
		,import_date
		,sourcesystem_cd
		,valuetype_cd
		,m_applied_path
		)
		select 1 as c_hlevel
			  ,'\' || 'Public Studies' || '\' || 'SECURITY' || '\' as c_fullname
			  ,'SECURITY' as c_name
			  ,'N' as c_synonym_cd
			  ,'CA' as c_visualattributes
			  ,null as c_totalnum
			  ,null as c_basecode
			  ,null as c_metadataxml
			  ,'concept_cd' as c_facttablecolumn
			  ,'concept_dimension' as c_tablename
			  ,'concept_path' as c_columnname
			  ,'T' as c_columndatatype
			  ,'LIKE' as c_operator
			  ,'\' || 'Public Studies' || '\' || 'SECURITY' || '\' as c_dimcode
			  ,null as c_comment
			  ,'\' || 'Public Studies' || '\' || 'SECURITY' || '\' as c_tooltip
			  ,etlDate as update_date
			  ,null as download_date
			  ,etlDate as import_date
			  ,null as sourcesystem_cd
			  ,null as valuetype_cd
			  ,'@' as m_applied_path;
		get diagnostics rowCt := ROW_COUNT;
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert \Public Studies\SECURITY\ node to i2b2',rowCt,stepCt,'Done')into rtnCd;	
	end if;

	--	check if SECURITY node exists in concept_dimension
	
	select count(*) into secNodeExists
	from i2b2demodata.concept_dimension
	where concept_path = '\' || 'Public Studies' || '\' || 'SECURITY' || '\';
	
	if secNodeExists = 0 then
		begin
		insert into i2b2demodata.concept_dimension
		(concept_cd
		,concept_path
		,name_char
		,update_date
		,download_date
		,import_date
		,sourcesystem_cd
		)
		select 'SECURITY'
			 ,'\' || 'Public Studies' || '\' || 'SECURITY' || '\'
			 ,'SECURITY'
			 ,etlDate
			 ,etlDate
			 ,etlDate
			 ,null;
		get diagnostics rowCt := ROW_COUNT;
		exception
		when others then
			errorNumber := SQLSTATE;
			errorMessage := SQLERRM;
			--Handle errors.
			select tm_cz.czx_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
			--End Proc
			select tm_cz.czx_end_audit (jobID, 'FAIL') into rtnCd;
			return -16;
		end;
		stepCt := stepCt + 1;
		select tm_cz.czx_write_audit(jobId,databaseName,procedureName,'Insert \Public Studies\SECURITY\ node to concept_dimension',rowCt,stepCt,'Done')into rtnCd;
	end if;

    ---Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		select tm_cz.czx_end_audit (jobID, 'SUCCESS')into rtnCd;
	END IF;

	return 0;


end;

$BODY$
  LANGUAGE plpgsql  SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.i2b2_load_security_data(numeric)
  OWNER TO postgres;
