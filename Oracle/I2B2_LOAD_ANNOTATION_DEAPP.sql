
  CREATE OR REPLACE PROCEDURE "I2B2_LOAD_ANNOTATION_DEAPP" 
(
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
	gplId	varchar2(100);

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
	cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_load_annotation_deapp',0,stepCt,'Done');

	--	get GPL id from external table
	
	select distinct gpl_id into gplId from lt_src_deapp_annot;
	
/*	
	--	delete any existing data from probeset_deapp
	
	delete from probeset_deapp
	where platform = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from REFERENCE probeset_deapp',SQL%ROWCOUNT,stepCt,'Done');
*/
		
	--	delete any existing data from annotation_deapp
	
	delete from annotation_deapp
	where gpl_id = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from annotation_deapp',SQL%ROWCOUNT,stepCt,'Done');

	--	delete any existing data from deapp.de_mrna_annotation
	
	delete from deapp.de_mrna_annotation
	where gpl_id = gplId;

	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Delete existing data from de_mrna_annotation',SQL%ROWCOUNT,stepCt,'Done');

	--	update organism for existing probesets in probeset_deapp
	
	update probeset_deapp p
	set organism=(select distinct t.organism from lt_src_deapp_annot t
				  where p.platform = t.gpl_id
				    and p.probeset = t.probe_id)
	where exists
		 (select 1 from lt_src_deapp_annot x
		  where p.platform = x.gpl_id
		    and p.probeset = x.probe_id);
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Update organism in probeset_deapp',SQL%ROWCOUNT,stepCt,'Done');
			
	--	insert any new probesets into probeset_deapp
	
	insert into probeset_deapp
	(probeset
	,organism
	,platform)
	select distinct probe_id
		  ,coalesce(organism,'Homo sapiens')
	      ,gpl_id
	from lt_src_deapp_annot t
	where not exists
		 (select 1 from probeset_deapp x
		  where t.gpl_id = x.platform
		    and t.probe_id = x.probeset
			and coalesce(t.organism,'Homo sapiens') = coalesce(x.organism,'Homo sapiens'))
	;
	--where gene_id is not null 
	--   or gene_symbol is not null;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Insert new probesets into probeset_deapp',SQL%ROWCOUNT,stepCt,'Done');
		
	--	insert data into annotation_deapp
	
	insert into annotation_deapp
	(gpl_id
	,probe_id
	,gene_symbol
	,gene_id
	,probeset_id
	,organism)
	select distinct d.gpl_id
	,d.probe_id
	,d.gene_symbol
	,d.gene_id
	,p.probeset_id
	,coalesce(d.organism,'Homo sapiens')
	from lt_src_deapp_annot d
	,probeset_deapp p
	where d.probe_id = p.probeset
	  and d.gpl_id = p.platform
	  and coalesce(d.organism,'Homo sapiens') = coalesce(p.organism,'Homo sapiens')
	  --and (d.gene_id is not null or d.gene_symbol is not null)
	  ;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load annotation data into REFERENCE annotation_deapp',SQL%ROWCOUNT,stepCt,'Done');
		
	--	insert data into deapp.de_mrna_annotation
	
	insert into de_mrna_annotation
	(gpl_id
	,probe_id
	,gene_symbol
	,gene_id
	,probeset_id
	,organism)
	select distinct d.gpl_id
	,d.probe_id
	,d.gene_symbol
	,decode(d.gene_id,null,null,to_number(d.gene_id)) as gene_id
	,p.probeset_id
	,coalesce(d.organism,'Homo sapiens')
	from lt_src_deapp_annot d
	,probeset_deapp p
	where d.probe_id = p.probeset
	  and d.gpl_id = p.platform
	  and coalesce(d.organism,'Homo sapiens') = coalesce(p.organism,'Homo sapiens')
	  --and (d.gene_id is not null or d.gene_symbol is not null)
	  ;
	
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Load annotation data into DEAPP de_mrna_annotation',SQL%ROWCOUNT,stepCt,'Done');
		
	--	update gene_id if null
	
	update de_mrna_annotation t
	set gene_id=(select to_number(min(b.primary_external_id)) as gene_id
				 from biomart.bio_marker b
				 where t.gene_symbol = b.bio_marker_name
				   and upper(b.organism) = upper(t.organism)
				   and upper(b.bio_marker_type) = 'GENE')
	where t.gpl_id = gplId
	  and t.gene_id is null
	  and t.gene_symbol is not null
	  and exists
		 (select 1 from biomart.bio_marker x
		  where t.gene_symbol = x.bio_marker_name
			and upper(x.organism) = upper(t.organism)
			and upper(x.bio_marker_type) = 'GENE');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated missing gene_id in de_mrna_annotation',SQL%ROWCOUNT,stepCt,'Done');
	
	--	update gene_symbol if null
	
	update de_mrna_annotation t
	set gene_symbol=(select min(b.bio_marker_name) as gene_symbol
				 from biomart.bio_marker b
				 where to_char(t.gene_id) = b.primary_external_id
				   and upper(b.organism) = upper(t.organism)
				   and upper(b.bio_marker_type) = 'GENE')
	where t.gpl_id = gplId
	  and t.gene_symbol is null
	  and t.gene_id is not null
	  and exists
		 (select 1 from biomart.bio_marker x
		  where to_char(t.gene_id) = x.primary_external_id
			and upper(x.organism) = upper(t.organism)
			and upper(x.bio_marker_type) = 'GENE');
			
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'Updated missing gene_id in de_mrna_annotation',SQL%ROWCOUNT,stepCt,'Done');
		
	stepCt := stepCt + 1;
	cz_write_audit(jobId,databaseName,procedureName,'End i2b2_load_annotation_deapp',0,stepCt,'Done');
	
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


/*

   CREATE SEQUENCE  "TM_CZ"."SEQ_PROBESET_ID"  MINVALUE 249738 MAXVALUE 99999999 INCREMENT BY 1 START WITH 265364 CACHE 20 NOORDER  NOCYCLE ;
   
   
  CREATE TABLE "TM_CZ"."ANNOTATION_DEAPP" 
   (	"GPL_ID" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"GENE_SYMBOL" VARCHAR2(100 BYTE), 
	"GENE_ID" VARCHAR2(100 BYTE), 
	"PROBESET_ID" NUMBER(38,0)
   ) PCTFREE 10  NOLOGGING
  TABLESPACE "TRANSMART" ;
  
  
  CREATE TABLE "TM_CZ"."PROBESET_DEAPP" 
   (	"PROBESET_ID" NUMBER(38,0) NOT NULL ENABLE, 
	"PROBESET" VARCHAR2(100 BYTE) NOT NULL ENABLE, 
	"PLATFORM" VARCHAR2(100 BYTE) NOT NULL ENABLE
   ) PCTFREE 10 NOLOGGING
  TABLESPACE "TRANSMART" ;
 

  CREATE INDEX "TM_CZ"."PROBESET_DEAPP_I1" ON "TM_CZ"."PROBESET_DEAPP" ("PROBESET_ID") 
  PCTFREE 10 NOLOGGING COMPUTE STATISTICS 
  TABLESPACE "INDX" ;
 
  CREATE INDEX "TM_CZ"."PROBESET_DEAPP_I2" ON "TM_CZ"."PROBESET_DEAPP" ("PROBESET", "PLATFORM") 
  PCTFREE 10 NOLOGGING COMPUTE STATISTICS 
  TABLESPACE "INDX" ;
 

  CREATE OR REPLACE TRIGGER "TM_CZ"."TRG_PROBESET_DEAPP" 
before insert on "PROBESET_DEAPP"    
	for each row begin     
		if inserting then       
			if :NEW."PROBESET_ID" is null then
				select SEQ_PROBESET_ID.nextval into :NEW."PROBESET_ID" from dual;       
			end if;   
		end if; 

ALTER TRIGGER "TM_CZ"."TRG_PROBESET_DEAPP" ENABLE;
 

  CREATE TABLE "TM_LZ"."DEAPP_ANNOT_EXTRNL" 
   (	"GPL_ID" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"GENE_SYMBOL" VARCHAR2(100 BYTE), 
	"GENE_ID" VARCHAR2(250 BYTE)
   ) 
   ORGANIZATION EXTERNAL 
    ( TYPE ORACLE_LOADER
      DEFAULT DIRECTORY "DATA"
      ACCESS PARAMETERS
      ( records delimited BY newline nologfile skip 1 fields terminated BY 0X'09' LRTRIM MISSING FIELD VALUES ARE NULL     )
      LOCATION
       ( 'GPL180_p.txt'
       )
    )
  ;
 
 
  CREATE TABLE "TM_LZ"."LT_SRC_DEAPP_ANNOT" 
   (	"GPL_ID" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"GENE_SYMBOL" VARCHAR2(100 BYTE), 
	"GENE_ID" VARCHAR2(250 BYTE),
	"ORGANISM" VARCHAR2(200)
   ) SEGMENT CREATION DEFERRED 
  PCTFREE 10 NOLOGGING
  TABLESPACE "TRANSMART" ;

 
  CREATE TABLE "DEAPP"."DE_GPL_INFO" 
   (	"PLATFORM" VARCHAR2(10 BYTE), 
	"TITLE" VARCHAR2(500 BYTE), 
	"ORGANISM" VARCHAR2(100 BYTE), 
	"ANNOTATION_DATE" TIMESTAMP (6)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS NOLOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "DEAPP" ;
 

  CREATE TABLE "DEAPP"."DE_MRNA_ANNOTATION" 
   (	"GPL_ID" VARCHAR2(100 BYTE), 
	"PROBE_ID" VARCHAR2(100 BYTE), 
	"GENE_SYMBOL" VARCHAR2(100 BYTE), 
	"PROBESET_ID" NUMBER(38,0), 
	"GENE_ID" NUMBER(18,0)
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS NOLOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "DEAPP" ;
 

  CREATE INDEX "DEAPP"."DE_MRNA_ANNOTATION_I2" ON "DEAPP"."DE_MRNA_ANNOTATION" ("GPL_ID", "PROBE_ID") 
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "DEAPP" ;
 


 
*/
