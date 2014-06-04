COPY DEAPP.DE_VARIANT_POPULATION_INFO    (
    DATASET_ID,
    info_name,
    type,
    "number",
    description
    )
FROM './load_variant_population_info.txt' DELIMITER E'\t';