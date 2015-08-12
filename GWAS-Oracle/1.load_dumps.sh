#!/bin/bash -e

set -x

cd "$DUMPS_LOCATION"
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=deapp.de_snp_gene_map.dmp  tables=de_snp_gene_map touser=deapp
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=deapp.de_rc_snp_info.dmp  tables=de_rc_snp_info touser=deapp
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=deapp.de_gene_info.dump  tables=de_gene_info touser=deapp
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=biomart.bio_recombination_rates.dump  tables=bio_recombination_rates touser=biomart

