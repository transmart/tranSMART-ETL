CREATE TABLE "TM_WZ"."WT_SUBJECT_RNA_PROBESET" 
   (	"PROBESET_ID" VARCHAR2(200), 
	"EXPR_ID" VARCHAR2(500 BYTE), 
	"INTENSITY_VALUE" NUMBER, 
	"NUM_CALLS" NUMBER, 
	"PVALUE" NUMBER, 
	"ASSAY_ID" NUMBER(18,0), 
	"PATIENT_ID" NUMBER(22,0), 
	"SAMPLE_ID" NUMBER(18,0), 
	"SUBJECT_ID" VARCHAR2(100 BYTE), 
	"TRIAL_NAME" VARCHAR2(200 BYTE), 
	"TIMEPOINT" VARCHAR2(200 BYTE), 
	"SAMPLE_TYPE" VARCHAR2(200 BYTE), 
	"PLATFORM" VARCHAR2(200 BYTE), 
	"TISSUE_TYPE" VARCHAR2(200 BYTE)
   )

	create or replace synonym tm_cz.WT_SUBJECT_RNA_PROBESET for "TM_WZ"."WT_SUBJECT_RNA_PROBESET" ;
	grant select, insert, update, delete on  "TM_WZ"."WT_SUBJECT_RNA_PROBESET"    to tm_cz;

-------------------------------------------------------------------------
truncate table deapp.de_subject_rna_data

alter table deapp.de_subject_rna_data modify PROBESET_ID  VARCHAR2(200) 
------------------------------------------------------------------------


alter table tm_wz.wt_subject_rna_logs modify PROBESET_ID  VARCHAR2(200)
alter table tm_wz.wt_subject_rna_med modify PROBESET_ID  VARCHAR2(200) 
alter table tm_wz.wt_subject_rna_calcs modify PROBESET_ID  VARCHAR2(200) 