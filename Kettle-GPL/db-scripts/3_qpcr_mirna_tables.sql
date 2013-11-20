--1."TM_LZ"."LT_SRC_QPCR_MIRNA_DATA" 
----------------------------------

CREATE TABLE "TM_LZ"."LT_SRC_QPCR_MIRNA_DATA" 
   (           "TRIAL_NAME" VARCHAR2(25 BYTE), 
                "PROBESET" VARCHAR2(100 BYTE), 
                "EXPR_ID" VARCHAR2(100 BYTE), 
                "INTENSITY_VALUE" VARCHAR2(50 BYTE)
   ) 

create or replace synonym tm_cz.LT_SRC_QPCR_MIRNA_DATA for "TM_LZ"."LT_SRC_QPCR_MIRNA_DATA" ;
grant select, insert, update, delete on  "TM_LZ"."LT_SRC_QPCR_MIRNA_DATA" to tm_cz;


--**************************************************************************************

--2."TM_LZ"."LT_SRC_MIRNA_SUBJ_SAMP_MAP" 
------------------------------------

CREATE TABLE "TM_LZ"."LT_SRC_MIRNA_SUBJ_SAMP_MAP" 
   (           "TRIAL_NAME" VARCHAR2(100 BYTE), 
                "SITE_ID" VARCHAR2(100 BYTE), 
                "SUBJECT_ID" VARCHAR2(100 BYTE), 
                "SAMPLE_CD" VARCHAR2(100 BYTE), 
                "PLATFORM" VARCHAR2(100 BYTE), 
                "TISSUE_TYPE" VARCHAR2(100 BYTE), 
                "ATTRIBUTE_1" VARCHAR2(256 BYTE), 
                "ATTRIBUTE_2" VARCHAR2(200 BYTE), 
                "CATEGORY_CD" VARCHAR2(200 BYTE), 
                "SOURCE_CD" VARCHAR2(200 BYTE)
   ) 


create or replace synonym tm_cz.LT_SRC_MIRNA_SUBJ_SAMP_MAP for "TM_LZ"."LT_SRC_MIRNA_SUBJ_SAMP_MAP" ;
grant select, insert, update, delete on "TM_LZ"."LT_SRC_MIRNA_SUBJ_SAMP_MAP" to tm_cz;


--**************************************************************************************

--3."TM_WZ"."WT_QPCR_MIRNA_NODES" 
-------------------------------


 CREATE TABLE "TM_WZ"."WT_QPCR_MIRNA_NODES" 
   (	"LEAF_NODE" VARCHAR2(2000 BYTE), 
	"CATEGORY_CD" VARCHAR2(2000 BYTE), 
	"PLATFORM" VARCHAR2(2000 BYTE), 
	"TISSUE_TYPE" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_1" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_2" VARCHAR2(2000 BYTE), 
	"TITLE" VARCHAR2(2000 BYTE), 
	"NODE_NAME" VARCHAR2(2000 BYTE), 
	"CONCEPT_CD" VARCHAR2(100 BYTE), 
	"TRANSFORM_METHOD" VARCHAR2(2000 BYTE), 
	"NODE_TYPE" VARCHAR2(50 BYTE)
   ) 


create or replace synonym tm_cz.WT_QPCR_MIRNA_NODES for "TM_WZ"."WT_QPCR_MIRNA_NODES";
grant select, insert, update, delete on  "TM_WZ"."WT_QPCR_MIRNA_NODES"  to tm_cz;


--**************************************************************************************

--4."TM_WZ"."WT_QPCR_MIRNA_NODE_VALUES"
------------------------------------


  CREATE TABLE "TM_WZ"."WT_QPCR_MIRNA_NODE_VALUES" 
   (	"CATEGORY_CD" VARCHAR2(2000 BYTE), 
	"PLATFORM" VARCHAR2(2000 BYTE), 
	"TISSUE_TYPE" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_1" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_2" VARCHAR2(2000 BYTE), 
	"TITLE" VARCHAR2(2000 BYTE), 
	"TRANSFORM_METHOD" VARCHAR2(2000 BYTE)
   ) 


create or replace synonym tm_cz.WT_QPCR_MIRNA_NODE_VALUES for "TM_WZ"."WT_QPCR_MIRNA_NODE_VALUES";
grant select, insert, update, delete on "TM_WZ"."WT_QPCR_MIRNA_NODE_VALUES"  to tm_cz;


--************************************************************************************
--5."TM_WZ"."WT_SUBJECT_MIRNA_MED" 
-----------------------------------

  CREATE TABLE "TM_WZ"."WT_SUBJECT_MIRNA_MED" 
   (	"PROBESET_ID" VARCHAR2(1000 BYTE), 
	"INTENSITY_VALUE" NUMBER, 
	"LOG_INTENSITY" NUMBER, 
	"ASSAY_ID" NUMBER(18,0), 
	"PATIENT_ID" NUMBER(18,0), 
	"SAMPLE_ID" NUMBER(18,0), 
	"SUBJECT_ID" VARCHAR2(50 BYTE), 
	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"TIMEPOINT" VARCHAR2(100 BYTE), 
	"PVALUE" FLOAT(126), 
	"NUM_CALLS" NUMBER, 
	"MEAN_INTENSITY" NUMBER, 
	"STDDEV_INTENSITY" NUMBER, 
	"MEDIAN_INTENSITY" NUMBER, 
	"ZSCORE" NUMBER
   ) 


create or replace synonym tm_cz.WT_SUBJECT_MIRNA_MED for "TM_WZ"."WT_SUBJECT_MIRNA_MED" ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_MIRNA_MED"   to tm_cz;

--**************************************************************************************
 --6."DEAPP"."DE_SUBJECT_MIRNA_DATA" 
-----------------------------------


  CREATE TABLE "DEAPP"."DE_SUBJECT_MIRNA_DATA" 
   (	"TRIAL_SOURCE" VARCHAR2(200 BYTE), 
	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"PROBESET_ID" VARCHAR2(1000 BYTE), 
	"ASSAY_ID" NUMBER(18,0), 
	"PATIENT_ID" NUMBER(18,0), 
	"RAW_INTENSITY" NUMBER(18,4), 
	"LOG_INTENSITY" NUMBER(18,4), 
	"ZSCORE" NUMBER(18,4)
   ) 


create or replace synonym tm_cz.DE_SUBJECT_MIRNA_DATA for "DEAPP"."DE_SUBJECT_MIRNA_DATA"  ;
grant select, insert, update, delete on "DEAPP"."DE_SUBJECT_MIRNA_DATA" to tm_cz;


--**************************************************************************************

--7."TM_WZ"."WT_SUBJECT_MIRNA_LOGS"
---------------------------------

  
  CREATE TABLE "TM_WZ"."WT_SUBJECT_MIRNA_LOGS" 
   (	"PROBESET_ID" VARCHAR2(1000 BYTE), 
	"INTENSITY_VALUE" NUMBER, 
	"PVALUE" FLOAT(126), 
	"NUM_CALLS" NUMBER, 
	"ASSAY_ID" NUMBER(18,0), 
	"PATIENT_ID" NUMBER(18,0), 
	"SAMPLE_ID" NUMBER(18,0), 
	"SUBJECT_ID" VARCHAR2(50 BYTE), 
	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"TIMEPOINT" VARCHAR2(100 BYTE), 
	"LOG_INTENSITY" NUMBER
   ) 

create or replace synonym tm_cz.WT_SUBJECT_MIRNA_LOGS for "TM_WZ"."WT_SUBJECT_MIRNA_LOGS"  ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_MIRNA_LOGS" to tm_cz;

--************************************************************************************

--8."TM_WZ"."WT_SUBJECT_MIRNA_CALCS"
---------------------------------

  
  CREATE TABLE "TM_WZ"."WT_SUBJECT_MIRNA_CALCS" 
   (	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"PROBESET_ID" VARCHAR2(1000 BYTE), 
	"MEAN_INTENSITY" NUMBER, 
	"MEDIAN_INTENSITY" NUMBER, 
	"STDDEV_INTENSITY" NUMBER
   ) 

create or replace synonym tm_cz.WT_SUBJECT_MIRNA_CALCS for "TM_WZ"."WT_SUBJECT_MIRNA_CALCS"  ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_MIRNA_CALCS" to tm_cz;


--**************************************************************************************
--9."TM_WZ"."WT_SUBJECT_MIRNA_PROBESET" 
---------------------------------------



 CREATE TABLE "TM_WZ"."WT_SUBJECT_MIRNA_PROBESET" 
   (	"PROBESET_ID" VARCHAR2(1000 BYTE), 
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


create or replace synonym tm_cz.WT_SUBJECT_MIRNA_PROBESET for "TM_WZ"."WT_SUBJECT_MIRNA_PROBESET"   ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_MIRNA_PROBESET"  to tm_cz;

begin
  util_grant_all('TM_CZ','TABLES');
end;



 