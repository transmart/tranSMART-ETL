#!/bin/bash -e

set -x

cd "$DUMPS_LOCATION"
F=$(tempfile).sql

cat > $F <<EOD
DROP TABLE deapp.de_rc_snp_info;
DROP TABLE deapp.de_gene_info;
EOD
echo exit | sqlplus $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID @$F

imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=deapp.de_snp_gene_map.dmp  tables=de_snp_gene_map touser=deapp
# much faster if the table has been dropped before. I haven't investigated why
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID file=deapp.de_rc_snp_info.dmp  tables=de_rc_snp_info touser=deapp feedback=100000
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID file=deapp.de_gene_info.dump  tables=de_gene_info touser=deapp
imp $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID ignore=y file=biomart.bio_recombination_rates.dump  tables=bio_recombination_rates touser=biomart

cat > $F <<EOD
grant select on deapp.de_rc_snp_info to tm_cz, biomart_user;
grant select on deapp.de_gene_info to tm_cz, biomart_user;
EOD
echo exit | sqlplus $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID @$F
