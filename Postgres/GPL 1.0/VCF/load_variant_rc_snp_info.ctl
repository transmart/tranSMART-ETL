COPY DEAPP.DE_RC_SNP_INFO (
    RS_ID,
    REF,
    ALT,
    GENE_NAME,
    ENTREZ_ID,
    VARIATION_CLASS,
    STRAND,
    CLINSIG,
    DISEASE,
    GMAF,
    GENE_BIOTYPE,
    IMPACT,
    TRANSCRIPT_ID,
    FUNCTIONAL_CLASS,
    EFFECT,
    EXON_ID,
    AMINO_ACID_CHANGE,
    CODON_CHANGE
    )
FROM './load_variant_rc_snp_info_postgres.txt' DELIMITER E'\t';