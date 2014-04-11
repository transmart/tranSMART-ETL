CREATE TRIGGER trg_cz_job_id
  BEFORE INSERT
  ON tm_cz.cz_job_master
  FOR EACH ROW
  EXECUTE PROCEDURE tm_cz.tf_trg_cz_job_id();
  
CREATE TRIGGER trg_cz_seq_id
  BEFORE INSERT
  ON tm_cz.cz_job_audit
  FOR EACH ROW
  EXECUTE PROCEDURE tm_cz.tf_trg_cz_seq_id();
  
  
  CREATE SEQUENCE i2b2metadata.sq_i2b2_id
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
ALTER TABLE i2b2metadata.sq_i2b2_id
  OWNER TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.sq_i2b2_id TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.sq_i2b2_id TO tm_cz;


  CREATE SEQUENCE i2b2demodata.seq_encounter_num
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
ALTER TABLE i2b2demodata.seq_encounter_num
  OWNER TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.sq_i2b2_id TO i2b2metadata;
GRANT ALL ON TABLE i2b2metadata.sq_i2b2_id TO tm_cz;

alter table tm_wz.wt_subject_microarray_logs renamed intensity_value to raw_intensity

all regexp_replaces needed to have a 'g' at the end (this is specific to postgres)

dropped index for data loading

-- DROP INDEX deapp.de_microarray_data_idx1;

CREATE INDEX de_microarray_data_idx1
  ON deapp.de_subject_microarray_data
  USING btree
  (trial_name COLLATE pg_catalog."default", assay_id, probeset_id);
  
 create schema biomart_stage
 create login role biomart_stage
  drop table BIOMART_STAGE.BIO_ASSAY_ANALYSIS_EQTL;

CREATE TABLE BIOMART_STAGE.BIO_ASSAY_ANALYSIS_EQTL
(
	BIO_ASY_ANALYSIS_EQTL_ID BIGINT,
	BIO_ASSAY_ANALYSIS_ID BIGINT,
	RS_ID NATIONAL CHARACTER VARYING(50),
	GENE CHARACTER VARYING(50),
	P_VALUE_CHAR CHARACTER VARYING(100),
	CIS_TRANS CHARACTER VARYING(10),
	DISTANCE_FROM_GENE CHARACTER VARYING(10),
	ETL_ID BIGINT,
	EXT_DATA CHARACTER VARYING(4000)
);

drop table BIOMART_STAGE.BIO_ASSAY_ANALYSIS_GWAS;

CREATE TABLE BIOMART_STAGE.BIO_ASSAY_ANALYSIS_GWAS
(
	BIO_ASY_ANALYSIS_GWAS_ID BIGINT,
	BIO_ASSAY_ANALYSIS_ID BIGINT,
	RS_ID NATIONAL CHARACTER VARYING(50),
	P_VALUE_CHAR CHARACTER VARYING(100),
	ETL_ID BIGINT,
	EXT_DATA CHARACTER VARYING(4000)
);

-- Sequence: tm_lz.seq_etl_id

-- DROP SEQUENCE tm_lz.seq_etl_id;

CREATE SEQUENCE tm_lz.seq_etl_id
  INCREMENT 1
  MINVALUE 1
  MAXVALUE 9223372036854775807
  START 1
  CACHE 1;
ALTER TABLE tm_lz.seq_etl_id
  OWNER TO postgres;
GRANT ALL ON TABLE tm_lz.seq_etl_id TO postgres;
GRANT ALL ON TABLE tm_lz.seq_etl_id TO transmartdb;

-- Function: tm_lz.tf_trg_etl_id()

-- DROP FUNCTION tm_lz.tf_trg_etl_id();

CREATE OR REPLACE FUNCTION tm_lz.tf_trg_etl_id()
  RETURNS trigger AS
$BODY$
begin     
      if coalesce(NEW.ETL_ID::text, '') = '' then          
        select nextval('tm_lz.seq_etl_id') into NEW.ETL_ID ;       
      end if;       
       RETURN NEW;
  end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION tm_cl.tf_trg_etl_id()
  OWNER TO postgres;
  
CREATE TRIGGER trg_analysis_metadata_etl_id
  BEFORE INSERT
  ON tm_lz.lz_src_analysis_metadata
  FOR EACH ROW
  EXECUTE PROCEDURE tm_lz.tf_trg_etl_id();
  
  CREATE OR REPLACE FUNCTION biomart.tf_trg_baad_idx_id()
  RETURNS trigger AS
$BODY$
begin     
      if coalesce(NEW.bio_asy_analysis_data_idx_id::text, '') = '' then          
        select nextval('biomart.seq_bio_data_id') into NEW.bio_asy_analysis_data_idx_id;       
      end if;       
       RETURN NEW;
  end;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION biomart.tf_trg_baad_idx_id()
  OWNER TO postgres;
  
CREATE TRIGGER trg_baad_idx_id
  BEFORE INSERT
  ON biomart.bio_asy_analysis_data_idx
  FOR EACH ROW
  EXECUTE PROCEDURE biomart.tf_trg_baad_idx_id();
  
  alter table biomart.bio_assay_analysis_gwas add p_value double precision;
alter table biomart.bio_assay_analysis_gwas add log_p_value double precision;
alter table biomart.bio_assay_analysis_eqtl add p_value double precision;
alter table biomart.bio_assay_analysis_eqtl add log_p_value double precision;

