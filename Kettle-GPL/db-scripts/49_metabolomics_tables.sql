-- define table DEAPP.DE_METABOLITE_ANNOTATION
CREATE TABLE DEAPP.DE_METABOLITE_ANNOTATION
(
  ID NUMBER(*,0) not null,
  GPL_ID VARCHAR2(50 BYTE) not null,
  BIOCHEMICAL_NAME VARCHAR2(200 BYTE) not null,
  BIOMARKER_ID NVARCHAR2(200),
  HMDB_ID VARCHAR2(50 BYTE)
) 
NOLOGGING 
TABLESPACE "DEAPP";
comment on column DEAPP.DE_METABOLITE_ANNOTATION.ID is
'Unique identifier of this record';
comment on column DEAPP.DE_METABOLITE_ANNOTATION.GPL_ID is
'GPL ID; reference to the de_gpl_info table which has one record for each platform';
comment on column DEAPP.DE_METABOLITE_ANNOTATION.BIOCHEMICAL_NAME is
'HMDB_ID for the HMDB record representing this metabolite. This is represented in the data as well, and may be null if not present';
comment on column DEAPP.DE_METABOLITE_ANNOTATION.BIOMARKER_ID is
'Biomarker ID of this metabolite (HMDBID) in the dictionary. This ID points to primary_external_id in a record in biomart_bio_marker. This value may be null, if no HMDB ID is present.';
comment on column DEAPP.DE_METABOLITE_ANNOTATION.HMDB_ID is
'HMDB_ID for the HMDB record representing this metabolite. This is represented in the data as well, and may be null if not present';
-- define table DEAPP.de_metabolite_super_pathways
CREATE TABLE DEAPP.de_metabolite_super_pathways 
(
  ID NUMBER(*,0) not null,
  GPL_ID VARCHAR2(50 BYTE) not null,
  SUPER_PATHWAY_NAME VARCHAR2(200 BYTE)
) 
NOLOGGING 
TABLESPACE "DEAPP";
comment on column DEAPP.de_metabolite_super_pathways.ID is
'Unique identifier of this record';
comment on column DEAPP.de_metabolite_super_pathways.GPL_ID is
'GPL ID; reference to the de_gpl_info table which has one record for each platform';
comment on column DEAPP.de_metabolite_super_pathways.SUPER_PATHWAY_NAME is
'Name of the superpathway, as represented in the data';
-- define table DEAPP.de_metabolite_sub_pathways
CREATE TABLE DEAPP.de_metabolite_sub_pathways
(
  ID NUMBER(*,0) not null,
  GPL_ID VARCHAR2(50 BYTE) not null,
  SUB_PATHWAY_NAME VARCHAR2(200 BYTE) not null,
  SUPER_PATHWAY_ID NUMBER(*,0)
) 
NOLOGGING 
TABLESPACE "DEAPP";
comment on column DEAPP.de_metabolite_sub_pathways.ID is
'Unique identifier of this record';
comment on column DEAPP.de_metabolite_sub_pathways.GPL_ID is
'GPL ID; reference to the de_gpl_info table which has one record for each platform';
comment on column DEAPP.de_metabolite_sub_pathways.SUPER_PATHWAY_ID is
'ID of the superpathway that this subpathway belongs to (a record in de_metabolite_super_pathways table). If there is no super_pathway, this value may be null';
comment on column DEAPP.de_metabolite_sub_pathways.SUB_PATHWAY_NAME is
'Name of the subpathway, as represented in the data';
-- define table DEAPP.de_metabolite_sub_pway_metab
CREATE TABLE DEAPP.de_metabolite_sub_pway_metab
(
  METABOLITE_ID NUMBER(*,0) not null,
  SUB_PATHWAY_ID NUMBER(*,0) not null
) 
NOLOGGING 
TABLESPACE "DEAPP";
comment on column DEAPP.de_metabolite_sub_pway_metab.METABOLITE_ID is
'Reference for the metabolite, referencing a record in the de_metabolite_annotation';
comment on column DEAPP.de_metabolite_sub_pway_metab.SUB_PATHWAY_ID is
'Reference to the sub_pathway, referencing a record in the de_metabolite_sub_pathways table.';
 
-- define table DEAPP.DE_SUBJECT_METABOLOMICS_DATA
CREATE TABLE DEAPP.DE_SUBJECT_METABOLOMICS_DATA 
(
  TRIAL_SOURCE VARCHAR2(200 BYTE),
  TRIAL_NAME VARCHAR2(200 BYTE),
  METABOLITE_ANNOTATION_ID NUMBER(*, 0),
  ASSAY_ID NUMBER(*, 0),
  SUBJECT_ID VARCHAR2(100 BYTE),
  PATIENT_ID NUMBER(*, 0),
  RAW_INTENSITY NUMBER, 
  LOG_INTENSITY NUMBER, 
  ZSCORE NUMBER not null
) 
NOLOGGING 
TABLESPACE "DEAPP";
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.TRIAL_SOURCE is
'Source of the trial';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.TRIAL_NAME is
'Trial name/dataset name';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.ASSAY_ID is
'Assay ID. Refers to a record in DE_SUBJECT_SAMPLE_MAPPING table';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.SUBJECT_ID is
'Subject name as used in the input file. Please not that max length = 50';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.PATIENT_ID is
'Patient_ID for this data. Refers to i2b2demodata.patient_dimension';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.RAW_INTENSITY is
'The value to be stored.';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.LOG_INTENSITY is
'The log intensity to be stored or computed';
comment on column DEAPP.DE_SUBJECT_METABOLOMICS_DATA.ZSCORE is
'The (computed) zscore for this value';
-- add primary key to DEAPP.DE_METABOLITE_ANNOTATION_PK
ALTER TABLE DEAPP.DE_METABOLITE_ANNOTATION
ADD CONSTRAINT DE_METABOLITE_ANNOTATION_PK PRIMARY KEY (ID);
-- add primary key to DEAPP.de_metabolite_sub_pathways
ALTER TABLE DEAPP.de_metabolite_sub_pathways
ADD CONSTRAINT de_metabolite_sub_pathway_PK PRIMARY KEY (ID);
-- add primary key to DEAPP.de_metabolite_super_pathways
ALTER TABLE DEAPP.de_metabolite_super_pathways
ADD CONSTRAINT de_metabolite_super_pathway_PK PRIMARY KEY (ID);
-- add foreign keys for DEAPP.de_metabolite_sub_pathway
ALTER TABLE DEAPP.de_metabolite_sub_pathways 
ADD FOREIGN KEY (SUPER_PATHWAY_ID) 
REFERENCES DEAPP.de_metabolite_super_pathways(ID);
-- add foreign keys for DEAPP.DE_METABOLITE_ANNOTATION
ALTER TABLE DEAPP.DE_METABOLITE_ANNOTATION 
ADD FOREIGN KEY (BIOMARKER_ID) 
REFERENCES biomart.bio_marker(PRIMARY_EXTERNAL_ID);
-- add foreign keys for DEAPP.de_metabolite_sub_pway_metab
ALTER TABLE DEAPP.de_metabolite_sub_pway_metab 
ADD FOREIGN KEY (METABOLITE_ID) 
REFERENCES DEAPP.DE_METABOLITE_ANNOTATION(ID);
ALTER TABLE DEAPP.de_metabolite_sub_pway_metab 
ADD FOREIGN KEY (SUB_PATHWAY_ID) 
REFERENCES DEAPP.de_metabolite_sub_pathways(ID);
 
-- add foreign keys for DEAPP.DE_SUBJECT_METABOLOMICS_DATA
ALTER TABLE DEAPP.DE_SUBJECT_METABOLOMICS_DATA 
ADD FOREIGN KEY (METABOLITE_ANNOTATION_ID) 
REFERENCES DEAPP.DE_METABOLITE_ANNOTATION(ID);
-- Cannot create below reference to non-defined unique column
--ALTER TABLE DEAPP.DE_SUBJECT_METABOLOMICS_DATA 
--ADD FOREIGN KEY (ASSAY_ID) 
--REFERENCES DEAPP.DE_SUBJECT_SAMPLE_MAPPING(ASSAY_ID);
--
--ALTER TABLE DEAPP.DE_SUBJECT_METABOLOMICS_DATA 
--ADD FOREIGN KEY (PATIENT_ID) 
--REFERENCES I2B2DEMODATA.PATIENT_DIMENSION(PATIENT_NUM);
