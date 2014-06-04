-- Update bio_marker_correl_mv with identity metabolite correlation
DROP MATERIALIZED VIEW BIOMART.BIO_MARKER_CORREL_MV;
CREATE MATERIALIZED VIEW BIOMART.BIO_MARKER_CORREL_MV(
                BIO_MARKER_ID,
                ASSO_BIO_MARKER_ID,
                CORREL_TYPE,
                MV_ID )
            TABLESPACE "BIOMART"
            AS SELECT DISTINCT
                b.bio_marker_id,
                b.bio_marker_id AS asso_bio_marker_id,
                'GENE' AS correl_type,
                1 AS mv_id
            FROM
                    biomart.bio_marker b
            WHERE
                    b.bio_marker_type = 'GENE'
            UNION
            SELECT DISTINCT
                b.bio_marker_id,
                b.bio_marker_id AS asso_bio_marker_id,
                'PROTEIN' AS correl_type,
                4 AS mv_id
            FROM
                biomart.bio_marker b
            WHERE
               b.bio_marker_type = 'PROTEIN'
            UNION
            SELECT DISTINCT
                b.bio_marker_id,
                b.bio_marker_id AS asso_bio_marker_id,
                'MIRNA' AS correl_type,
                7 AS mv_id
            FROM
                biomart.bio_marker b
            WHERE
               b.bio_marker_type = 'MIRNA'
            UNION
            SELECT DISTINCT
                c.bio_data_id AS bio_marker_id,
                c.asso_bio_data_id AS asso_bio_marker_id,
                'PATHWAY GENE' AS correl_type,
                2 AS mv_id
            FROM
                biomart.bio_marker b,
                biomart.bio_data_correlation c,
                biomart.bio_data_correl_descr d
            WHERE
                b.bio_marker_id = c.bio_data_id
                AND c.bio_data_correl_descr_id = d.bio_data_correl_descr_id
                AND b.primary_source_code <> 'ARIADNE'
                AND d.correlation = 'PATHWAY GENE'
            UNION
            SELECT DISTINCT
                c.bio_data_id AS bio_marker_id,
                c.asso_bio_data_id AS asso_bio_marker_id,
                'HOMOLOGENE_GENE' AS correl_type,
                3 AS mv_id
            FROM
                biomart.bio_marker b,
                biomart.bio_data_correlation c,
                biomart.bio_data_correl_descr d
            WHERE
                b.bio_marker_id = c.bio_data_id
                AND c.bio_data_correl_descr_id = d.bio_data_correl_descr_id
                AND d.correlation = 'HOMOLOGENE GENE'
            UNION
            SELECT DISTINCT
                c.bio_data_id AS bio_marker_id,
                c.asso_bio_data_id AS asso_bio_marker_id,
                'PROTEIN TO GENE' AS correl_type,
                5 AS mv_id
            FROM
                biomart.bio_marker b,
                biomart.bio_data_correlation c,
                biomart.bio_data_correl_descr d
            WHERE
                b.bio_marker_id = c.bio_data_id
                AND c.bio_data_correl_descr_id = d.bio_data_correl_descr_id
                AND d.correlation = 'PROTEIN TO GENE'
            UNION
            SELECT DISTINCT
                c.bio_data_id AS bio_marker_id,
                c.asso_bio_data_id AS asso_bio_marker_id,
                'GENE TO PROTEIN' AS correl_type,
                6 AS mv_id
            FROM
                biomart.bio_marker b,
                biomart.bio_data_correlation c,
                biomart.bio_data_correl_descr d
            WHERE
                b.bio_marker_id = c.bio_data_id
                AND c.bio_data_correl_descr_id = d.bio_data_correl_descr_id
                AND d.correlation = 'GENE TO PROTEIN'
            UNION
            SELECT
                c1.bio_data_id,
                c2.asso_bio_data_id,
                'PATHWAY TO PROTEIN' as correl_type,
                8 AS mv_id
            FROM
                bio_data_correlation c1
                INNER JOIN bio_data_correlation c2 ON c1.asso_bio_data_id = c2.bio_data_id
                INNER JOIN bio_data_correl_descr d1 ON c1.bio_data_correl_descr_id = d1.bio_data_correl_descr_id
                INNER JOIN bio_data_correl_descr d2 ON c2.bio_data_correl_descr_id = d2.bio_data_correl_descr_id
                WHERE d1.correlation = 'PATHWAY GENE'
                AND d2.correlation = 'GENE TO PROTEIN'
            UNION
            SELECT DISTINCT
                b.bio_marker_id,
                b.bio_marker_id AS asso_bio_marker_id,
                'METABOLITE' AS correl_type,
                9 AS mv_id
            FROM
                biomart.bio_marker b
            WHERE
               b.bio_marker_type = 'METABOLITE';
GRANT SELECT ON BIOMART.BIO_MARKER_CORREL_MV TO biomart_user;

-- Correlation view for subpathways
GRANT SELECT ON deapp.de_metabolite_sub_pathways TO biomart;
GRANT SELECT ON deapp.de_metabolite_sub_pway_metab TO biomart;
GRANT SELECT ON deapp.de_metabolite_annotation TO biomart;
CREATE VIEW BIOMART.BIO_METAB_SUBPATHWAY_VIEW(
                SUBPATHWAY_ID,
                ASSO_BIO_MARKER_ID,
                CORREL_TYPE) AS
            SELECT
                SP.id,
                B.bio_marker_id,
                'SUBPATHWAY TO METABOLITE'
            FROM
                deapp.de_metabolite_sub_pathways SP
                INNER JOIN deapp.de_metabolite_sub_pway_metab J ON (SP.id = J.sub_pathway_id)
                INNER JOIN deapp.de_metabolite_annotation M ON (M.id = J.metabolite_id)
                INNER JOIN biomart.bio_marker B ON (
                    B.bio_marker_type = 'METABOLITE' AND
                    B.primary_external_id = M.hmdb_id);
GRANT SELECT ON DEAPP.DE_METABOLITE_ANNOTATION to biomart WITH GRANT OPTION;
GRANT SELECT ON DEAPP.DE_METABOLITE_SUB_PWAY_METAB to biomart WITH GRANT OPTION;
GRANT SELECT ON DEAPP.DE_METABOLITE_SUB_PATHWAYS to biomart WITH GRANT OPTION;
GRANT SELECT ON DEAPP.DE_METABOLITE_SUPER_PATHWAYS to biomart WITH GRANT OPTION;
GRANT SELECT ON BIOMART.BIO_METAB_SUBPATHWAY_VIEW to biomart_user;

-- Correlation view for superpathways
GRANT SELECT ON deapp.de_metabolite_super_pathways TO biomart;
CREATE VIEW BIOMART.BIO_METAB_SUPERPATHWAY_VIEW(
                SUPERPATHWAY_ID,
                ASSO_BIO_MARKER_ID,
                CORREL_TYPE) AS
            SELECT
                SUPP.id,
                B.bio_marker_id,
                'SUPERPATHWAY TO METABOLITE'
            FROM
                deapp.de_metabolite_super_pathways SUPP
                INNER JOIN deapp.de_metabolite_sub_pathways SUBP ON (SUPP.id = SUBP.super_pathway_id)
                INNER JOIN deapp.de_metabolite_sub_pway_metab J ON (SUBP.id = J.sub_pathway_id)
                INNER JOIN deapp.de_metabolite_annotation M ON (M.id = J.metabolite_id)
                INNER JOIN biomart.bio_marker B ON (
                    B.bio_marker_type = 'METABOLITE' AND
                    B.primary_external_id = M.hmdb_id);
GRANT SELECT ON BIOMART.BIO_METAB_SUPERPATHWAY_VIEW to biomart_user;
