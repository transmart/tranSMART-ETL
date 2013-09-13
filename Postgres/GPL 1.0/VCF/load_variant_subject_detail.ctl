COPY DEAPP.DE_VARIANT_SUBJECT_DETAIL    (
    DATASET_ID,
    CHR,
    POS,
    RS_ID,
    REF,
    ALT,
    QUAL,
    FILTER,
    INFO,
    FORMAT,
    VARIANT_VALUE
    )
FROM './load_variant_subject_detail_postgres.txt' DELIMITER '^';