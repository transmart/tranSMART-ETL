COPY DEAPP.DE_VARIANT_METADATA
  ( DATASET_ID, KEY, VALUE )
    FROM './load_variant_metadata.txt' DELIMITER E'\t';
