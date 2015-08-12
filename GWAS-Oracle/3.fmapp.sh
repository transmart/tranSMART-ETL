#!/bin/bash -e

echo exit | sqlplus $ORAUSER/$ORAPASSWORD@$ORAHOST:$ORAPORT/$ORASID @3.fmapp.sql
echo exit | sqlplus deapp/deapp@$ORAHOST:$ORAPORT/$ORASID @3.indexes.sql

