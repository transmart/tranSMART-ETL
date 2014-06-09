
 CREATE TABLE "TM_LZ"."LT_SRC_RBM_DATA" 
   (	"SAMPLE_ID" VARCHAR2(200 BYTE), 
	"ANALYTE" VARCHAR2(200 BYTE), 
	"AVALUE" VARCHAR2(200 BYTE), 
	"TRIAL_ID" VARCHAR2(20 BYTE)
   )

	create or replace synonym tm_cz.LT_SRC_RBM_DATA for "TM_LZ"."LT_SRC_RBM_DATA" ;
	grant select, insert, update, delete on "TM_LZ"."LT_SRC_RBM_DATA" to tm_cz;

------------------------------------------------------------------------------------

CREATE TABLE "TM_LZ"."LT_SRC_RBM_SUBJ_SAMP_MAP" 
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

	create or replace synonym tm_cz.LT_SRC_RBM_SUBJ_SAMP_MAP for "TM_LZ"."LT_SRC_RBM_SUBJ_SAMP_MAP" ;
	grant select, insert, update, delete on "TM_LZ"."LT_SRC_RBM_SUBJ_SAMP_MAP" to tm_cz;

---------------------------------------------------------------------



 CREATE TABLE "TM_WZ"."WT_RBM_NODES" 
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
   


	create or replace synonym tm_cz.WT_RBM_NODES for "TM_WZ"."WT_RBM_NODES";
	grant select, insert, update, delete on  "TM_WZ"."WT_RBM_NODES"  to tm_cz;

----------------------------------------------------------------------------
     CREATE TABLE "TM_WZ"."WT_RBM_NODE_VALUES" 
   (	"CATEGORY_CD" VARCHAR2(2000 BYTE), 
	"PLATFORM" VARCHAR2(2000 BYTE), 
	"TISSUE_TYPE" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_1" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_2" VARCHAR2(2000 BYTE), 
	"TITLE" VARCHAR2(2000 BYTE), 
	"TRANSFORM_METHOD" VARCHAR2(2000 BYTE)
   ) 



	create or replace synonym tm_cz.WT_RBM_NODE_VALUES for "TM_WZ"."WT_RBM_NODE_VALUES";
	grant select, insert, update, delete on "TM_WZ"."WT_RBM_NODE_VALUES"  to tm_cz;

---------------------------------------------------------------------------


  CREATE TABLE "TM_WZ"."WT_SUBJECT_RBM_PROBESET" 
   (	"PROBESET" VARCHAR2(1000 BYTE), 
	"EXPR_ID" VARCHAR2(500 BYTE), 
	"INTENSITY_VALUE" NUMBER, 
	"NUM_CALLS" NUMBER, 
	"PVALUE" NUMBER, 
	"ASSAY_ID" NUMBER(18,0), 
	"PATIENT_ID" NUMBER(22,0), 
	"SAMPLE_ID" VARCHAR2(100 BYTE), 
	"SUBJECT_ID" VARCHAR2(100 BYTE), 
	"TRIAL_NAME" VARCHAR2(200 BYTE), 
	"TIMEPOINT" VARCHAR2(200 BYTE), 
	"SAMPLE_TYPE" VARCHAR2(200 BYTE), 
	"PLATFORM" VARCHAR2(200 BYTE), 
	"TISSUE_TYPE" VARCHAR2(200 BYTE)
   )


	create or replace synonym tm_cz.WT_SUBJECT_RBM_PROBESET for "TM_WZ"."WT_SUBJECT_RBM_PROBESET" ;
	grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_RBM_PROBESET"   to tm_cz;
----------------------------------------------------------------------------


 CREATE TABLE "TM_WZ"."WT_SUBJECT_RBM_MED" 
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




	create or replace synonym tm_cz.WT_SUBJECT_RBM_MED for "TM_WZ"."WT_SUBJECT_RBM_MED" ;
	grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_RBM_MED"   to tm_cz;
----------------------------------------------------------------------------


CREATE TABLE "TM_WZ"."WT_SUBJECT_RBM_LOGS" 
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
	create or replace synonym tm_cz.WT_SUBJECT_RBM_LOGS for "TM_WZ"."WT_SUBJECT_RBM_LOGS"  ;
	grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_RBM_LOGS" to tm_cz;


---------------------------------------------------------------------------



 CREATE TABLE "TM_WZ"."WT_SUBJECT_RBM_CALCS" 
   (	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"PROBESET_ID" VARCHAR2(1000 BYTE), 
	"MEAN_INTENSITY" NUMBER, 
	"MEDIAN_INTENSITY" NUMBER, 
	"STDDEV_INTENSITY" NUMBER
   ) 

	create or replace synonym tm_cz.WT_SUBJECT_RBM_CALCS for "TM_WZ"."WT_SUBJECT_RBM_CALCS"  ;
	grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_RBM_CALCS" to tm_cz;


	create or replace synonym tm_cz.DE_SUBJECT_RBM_DATA for "DEAPP"."DE_SUBJECT_RBM_DATA"  ;
	grant select, insert, update, delete on "DEAPP"."DE_SUBJECT_RBM_DATA" to tm_cz;

---------------------------------------------------------------------------------