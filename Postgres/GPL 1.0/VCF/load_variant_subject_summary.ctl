COPY DEAPP.DE_VARIANT_SUBJECT_SUMMARY    (
    CHR,
    POS,
    DATASET_ID,
    SUBJECT_ID,
    RS_ID,
    VARIANT,
    VARIANT_FORMAT,
    REFERENCE,
    VARIANT_TYPE,
    ALLELE1,
    ALLELE2
    )
FROM './load_variant_subject_summary.txt' DELIMITER E'\t';


