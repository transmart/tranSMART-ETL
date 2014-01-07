create or replace 
PACKAGE valid AS

    TYPE data_violation_record IS RECORD(
       lvl VARCHAR2(50), 
       data_type VARCHAR2(50),
       message VARCHAR2(800));

    TYPE data_violation_table IS TABLE OF data_violation_record;

    FUNCTION validate_hd_data
        RETURN data_violation_table
        PIPELINED;
END;

create or replace 
PACKAGE BODY valid AS

    FUNCTION validate_hd_data
        RETURN data_violation_table
        PIPELINED IS

        rec data_violation_record;

    BEGIN
    
        ------ Uniqness of ASSAY_ID and annotation in data ------
        
        --MIRNA
        FOR v_rec IN (
          select assay_id, probeset_id, count(*) cnt
          from DEAPP.de_subject_mirna_data
          group by assay_id, probeset_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_subject_mirna_data', 'Expected 1 but there are ' || v_rec.cnt || ' records where assay_id=' || v_rec.assay_id || ' and probeset_id=' || v_rec.probeset_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;

        --RNA
        FOR v_rec IN (
          select assay_id, probeset_id, count(*) cnt
          from DEAPP.de_subject_rna_data
          group by assay_id, probeset_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_subject_rna_data', 'Expected 1 but there are ' || v_rec.cnt || ' records where assay_id=' || v_rec.assay_id || ' and probeset_id=' || v_rec.probeset_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;
        
        --PROTEOMICS
        FOR v_rec IN (
          select assay_id, protein_annotation_id, count(*) cnt
          from DEAPP.de_subject_protein_data
          group by assay_id, protein_annotation_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_subject_protein_data', 'Expected 1 but there are ' || v_rec.cnt || ' records where assay_id=' || v_rec.assay_id || ' and protein_annotation_id=' || v_rec.protein_annotation_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;
        
        --RBM
        FOR v_rec IN (
          select assay_id, rbm_annotation_id, count(*) cnt
          from DEAPP.de_subject_rbm_data
          group by assay_id, rbm_annotation_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_subject_rbm_data', 'Expected 1 but there are ' || v_rec.cnt || ' records where assay_id=' || v_rec.assay_id || ' and rbm_annotation_id=' || v_rec.rbm_annotation_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;

        ------ Uniqness of annotation rows ------
        
        --MIRNA
        FOR v_rec IN (
          select probeset_id, count(*) cnt
          from DEAPP.de_qpcr_mirna_annotation
          group by probeset_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_qpcr_mirna_annotation', 'Expected 1 but there are ' || v_rec.cnt || ' records with the same probeset_id=' || v_rec.probeset_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;
        
        --RNA
        FOR v_rec IN (
          select transcript_id, count(*) cnt
          from DEAPP.de_rnaseq_annotation
          group by transcript_id
          having count(*) > 1
          order by cnt desc
         )
        LOOP
          SELECT 'ERROR', 'de_rnaseq_annotation', 'Expected 1 but there are ' || v_rec.cnt || ' records with the same transcript_id=' || v_rec.transcript_id
          INTO rec
          FROM DUAL;

          PIPE ROW (rec);
        END LOOP;
        
        ------ Check how HD data maps to i2b2 ------
        
        --MIRNA
        FOR v_rec IN (
          select d.assay_id, d.probeset_id, ssm.assay_id as ssm_assay_id, p.patient_num as pd_patient_num, c.concept_cd as cd_concept_cd
          from DEAPP.de_subject_mirna_data d
          left join DEAPP.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id
          left join I2B2DEMODATA.patient_dimension p on p.patient_num = ssm.patient_id
          left join I2B2DEMODATA.concept_dimension c on c.concept_cd = c.concept_cd
          where ssm.assay_id is null or p.patient_num is null or c.concept_cd is null
         )
        LOOP
          IF v_rec.ssm_assay_id IS NULL THEN
            SELECT 'ERROR', 'de_subject_mirna_data', 'miRNA row with assay_id=' || v_rec.assay_id || ' and probeset_id=' || v_rec.probeset_id || ' has no related row in de_subject_sample_mapping'
            INTO rec FROM DUAL;
          ELSIF v_rec.pd_patient_num IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no patients for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          ELSIF v_rec.cd_concept_cd IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no such concept=' || v_rec.assay_id || ' for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          END IF;

          PIPE ROW (rec);
        END LOOP;
        
        --RNA
        FOR v_rec IN (
          select d.assay_id, d.probeset_id, ssm.assay_id as ssm_assay_id, p.patient_num as pd_patient_num, c.concept_cd as cd_concept_cd
          from DEAPP.de_subject_rna_data d
          left join DEAPP.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id
          left join I2B2DEMODATA.patient_dimension p on p.patient_num = ssm.patient_id
          left join I2B2DEMODATA.concept_dimension c on c.concept_cd = c.concept_cd
          where ssm.assay_id is null or p.patient_num is null or c.concept_cd is null
         )
        LOOP
          IF v_rec.ssm_assay_id IS NULL THEN
            SELECT 'ERROR', 'de_subject_rna_data', 'RNA row with assay_id=' || v_rec.assay_id || ' and probeset_id=' || v_rec.probeset_id || ' has no related row in de_subject_sample_mapping'
            INTO rec FROM DUAL;
          ELSIF v_rec.pd_patient_num IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no patients for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          ELSIF v_rec.cd_concept_cd IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no such concept=' || v_rec.assay_id || ' for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          END IF;

          PIPE ROW (rec);
        END LOOP;
        
        --PROTEOMICS
        FOR v_rec IN (
          select d.assay_id, d.protein_annotation_id, ssm.assay_id as ssm_assay_id, p.patient_num as pd_patient_num, c.concept_cd as cd_concept_cd
          from DEAPP.de_subject_protein_data d
          left join DEAPP.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id
          left join I2B2DEMODATA.patient_dimension p on p.patient_num = ssm.patient_id
          left join I2B2DEMODATA.concept_dimension c on c.concept_cd = c.concept_cd
          where ssm.assay_id is null or p.patient_num is null or c.concept_cd is null
         )
        LOOP
          IF v_rec.ssm_assay_id IS NULL THEN
            SELECT 'ERROR', 'de_subject_protein_data', 'Protein row with assay_id=' || v_rec.assay_id || ' and protein_annotation_id=' || v_rec.protein_annotation_id || ' has no related row in de_subject_sample_mapping'
            INTO rec FROM DUAL;
          ELSIF v_rec.pd_patient_num IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no patients for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          ELSIF v_rec.cd_concept_cd IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no such concept=' || v_rec.assay_id || ' for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          END IF;

          PIPE ROW (rec);
        END LOOP;
        
        --RBM
        FOR v_rec IN (
          select d.assay_id, d.rbm_annotation_id, ssm.assay_id as ssm_assay_id, p.patient_num as pd_patient_num, c.concept_cd as cd_concept_cd
          from DEAPP.de_subject_rbm_data d
          left join DEAPP.de_subject_sample_mapping ssm on ssm.assay_id = d.assay_id
          left join I2B2DEMODATA.patient_dimension p on p.patient_num = ssm.patient_id
          left join I2B2DEMODATA.concept_dimension c on c.concept_cd = c.concept_cd
          where ssm.assay_id is null or p.patient_num is null or c.concept_cd is null
         )
        LOOP
          IF v_rec.ssm_assay_id IS NULL THEN
            SELECT 'ERROR', 'de_subject_rbm_data', 'RBM row with assay_id=' || v_rec.assay_id || ' and rbm_annotation_id=' || v_rec.rbm_annotation_id || ' has no related row in de_subject_sample_mapping'
            INTO rec FROM DUAL;
          ELSIF v_rec.pd_patient_num IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no patients for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          ELSIF v_rec.cd_concept_cd IS NULL THEN
            SELECT 'ERROR', 'de_subject_sample_mapping', 'There is no such concept=' || v_rec.assay_id || ' for assay_id=' || v_rec.assay_id
            INTO rec FROM DUAL;
          END IF;

          PIPE ROW (rec);
        END LOOP;
        
        RETURN;
    END validate_hd_data;
END;

CREATE OR REPLACE VIEW validate_hd_data_view AS
select * from table(valid.validate_hd_data);
