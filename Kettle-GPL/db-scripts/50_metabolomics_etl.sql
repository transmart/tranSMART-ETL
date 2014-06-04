 CREATE TABLE "TM_WZ"."WT_METABOLOMIC_NODES" 
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

create or replace synonym tm_cz.WT_METABOLOMIC_NODES for "TM_WZ"."WT_METABOLOMIC_NODES";
grant select, insert, update, delete on "TM_WZ"."WT_METABOLOMIC_NODES" to tm_cz;

-------------------------------------------
-------------------------------------------
CREATE TABLE "TM_LZ"."LT_SRC_METABOLOMIC_MAP" 
   (	"TRIAL_NAME" VARCHAR2(100 BYTE), 
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

create or replace synonym tm_cz.LT_SRC_METABOLOMIC_MAP for "TM_LZ"."LT_SRC_METABOLOMIC_MAP";
grant select, insert, update, delete on "TM_LZ"."LT_SRC_METABOLOMIC_MAP" to tm_cz;

-----------------------------------------
-----------------------------------------

CREATE TABLE "TM_WZ"."WT_METABOLOMIC_NODE_VALUES" 
   (	"CATEGORY_CD" VARCHAR2(2000 BYTE), 
	"PLATFORM" VARCHAR2(2000 BYTE), 
	"TISSUE_TYPE" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_1" VARCHAR2(2000 BYTE), 
	"ATTRIBUTE_2" VARCHAR2(2000 BYTE), 
	"TITLE" VARCHAR2(2000 BYTE), 
	"TRANSFORM_METHOD" VARCHAR2(2000 BYTE)
   ) 

create or replace synonym tm_cz.WT_METABOLOMIC_NODE_VALUES for "TM_WZ"."WT_METABOLOMIC_NODE_VALUES";
grant select, insert, update, delete on "TM_WZ"."WT_METABOLOMIC_NODE_VALUES" to tm_cz;
------------------------------------------
------------------------------------------

CREATE TABLE "TM_WZ"."WT_SUBJECT_MBOLOMICS_PROBESET" 
   (	"PROBESET" VARCHAR2(500 BYTE), 
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


create or replace synonym tm_cz.WT_SUBJECT_MBOLOMICS_PROBESET for "TM_WZ"."WT_SUBJECT_MBOLOMICS_PROBESET";
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_MBOLOMICS_PROBESET" to tm_cz;

----------------------------------------
----------------------------------------  


CREATE TABLE "TM_LZ"."LT_SRC_METABOLOMIC_DATA" 
   (	"TRIAL_NAME" VARCHAR2(25 BYTE), 
        "BIOCHEMICAL" VARCHAR2(200 BYTE),
        "EXPR_ID"  VARCHAR2(100 BYTE),
        "INTENSITY_VALUE" VARCHAR2(50 BYTE)
  )

create or replace synonym tm_cz.LT_SRC_METABOLOMIC_DATA for "TM_LZ"."LT_SRC_METABOLOMIC_DATA";
grant select, insert, update, delete on  "TM_LZ"."LT_SRC_METABOLOMIC_DATA" to tm_cz;
-------------------------------------------------------

create or replace synonym tm_cz.DE_SUBJECT_METABOLOMICS_DATA for "DEAPP"."DE_SUBJECT_METABOLOMICS_DATA" ;
grant select, insert, update, delete on  "DEAPP"."DE_SUBJECT_METABOLOMICS_DATA" to tm_cz;
----------------------------------------------------------------------------
----------------------------------------------------------------------------

CREATE TABLE "TM_WZ"."WT_SUBJECT_METABOLOMICS_MED" 
   (	"PROBESET" VARCHAR2(500 BYTE), 
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

create or replace synonym tm_cz.WT_SUBJECT_METABOLOMICS_MED for "TM_WZ"."WT_SUBJECT_METABOLOMICS_MED" ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_METABOLOMICS_MED" to tm_cz;
----------------------------------------------------------------------------


CREATE TABLE "TM_WZ"."WT_SUBJECT_METABOLOMICS_LOGS" 
   (	"PROBESET" VARCHAR2(500 BYTE), 
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

create or replace synonym tm_cz.WT_SUBJECT_METABOLOMICS_LOGS for "TM_WZ"."WT_SUBJECT_METABOLOMICS_LOGS" ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_METABOLOMICS_LOGS" to tm_cz;


---------------------------------------------------------------------------

 CREATE TABLE "TM_WZ"."WT_SUBJECT_METABOLOMICS_CALCS" 
   (	"TRIAL_NAME" VARCHAR2(50 BYTE), 
	"PROBESET" VARCHAR2(500 BYTE), 
	"MEAN_INTENSITY" NUMBER, 
	"MEDIAN_INTENSITY" NUMBER, 
	"STDDEV_INTENSITY" NUMBER
   ) 

create or replace synonym tm_cz.WT_SUBJECT_METABOLOMICS_CALCS for "TM_WZ"."WT_SUBJECT_METABOLOMICS_CALCS" ;
grant select, insert, update, delete on "TM_WZ"."WT_SUBJECT_METABOLOMICS_CALCS" to tm_cz;
-----------------------------------------------------------------------------------------------


--define table LT_METABOLOMIC_ANNOTATION
create table "TM_LZ"."LT_METABOLOMIC_ANNOTATION"
(
GPL_ID varchar2(20),
BIOCHEMICAL_NAME varchar2(200),
HMDB_ID varchar2(20),
SUPER_PATHWAY varchar2(200),
SUB_PATHWAY varchar2(200)
)

create or replace synonym tm_cz.LT_METABOLOMIC_ANNOTATION for "TM_LZ"."LT_METABOLOMIC_ANNOTATION" ;
grant select, insert, update, delete on "TM_LZ"."LT_METABOLOMIC_ANNOTATION" to tm_cz;
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

create or replace synonym tm_cz.de_metabolite_annotation for "DEAPP"."DE_METABOLITE_ANNOTATION";
grant select, insert, update, delete on "DEAPP"."DE_METABOLITE_ANNOTATION" to tm_cz;

--------------------------------------------------------------------

CREATE SEQUENCE  "DEAPP"."METABOLOMICS_ANNOT_ID"  MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE ;

create or replace synonym tm_cz.metabolomics_annot_id for "DEAPP"."METABOLOMICS_ANNOT_ID";
grant select, alter on "DEAPP"."METABOLOMICS_ANNOT_ID" to tm_cz;

create or replace synonym tm_cz.de_metabolite_super_pathways for "DEAPP"."DE_METABOLITE_SUPER_PATHWAYS";
grant select, insert, update, delete on "DEAPP"."DE_METABOLITE_SUPER_PATHWAYS" to tm_cz;

CREATE SEQUENCE  "DEAPP"."METABOLITE_SUP_PTH_ID"  MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE;

create or replace synonym tm_cz.metabolite_sup_pth_id for "DEAPP"."METABOLITE_SUP_PTH_ID";
grant select, alter on "DEAPP"."METABOLITE_SUP_PTH_ID" to tm_cz;


create or replace synonym tm_cz.de_metabolite_sub_pathways for "DEAPP"."DE_METABOLITE_SUB_PATHWAYS";
grant select, insert, update, delete on "DEAPP"."DE_METABOLITE_SUB_PATHWAYS" to tm_cz;

CREATE SEQUENCE  "DEAPP"."METABOLITE_SUB_PTH_ID"  MINVALUE 1 MAXVALUE 999999999 INCREMENT BY 1 START WITH 1 NOCACHE  NOORDER  NOCYCLE;

create or replace synonym tm_cz.metabolite_sub_pth_id for "DEAPP"."METABOLITE_SUB_PTH_ID";
grant select, alter on "DEAPP"."METABOLITE_SUB_PTH_ID" to tm_cz;

create or replace synonym tm_cz.de_metabolite_sub_pway_metab for "DEAPP"."DE_METABOLITE_SUB_PWAY_METAB";
grant select, insert, update, delete on "DEAPP"."DE_METABOLITE_SUB_PWAY_METAB" to tm_cz;


