COPY DEAPP.DE_VARIANT_POPULATION_DATA   (
    DATASET_ID,
    CHR,
    POS,
    INFO_NAME,
    INFO_INDEX,
    integer_value,
    float_value,
    text_value
    )
FROM './load_variant_population_data.txt' DELIMITER E'\t';