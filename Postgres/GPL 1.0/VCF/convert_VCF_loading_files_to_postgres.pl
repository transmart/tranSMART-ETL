#!/usr/bin/perl

use strict;

## This script converts the VCF loading files into a format that can be used for postgres
## This script is deliberately kept separate from the generate_VCF_loading_files script,
## so that script can be used both for oracle and postgres 

## Basically, this script does 2 things:
## 1. Converts the load_variant_subject_detail.txt
##   By default, it has two characteristics that have to be changed for postgres:
##     - The delimiter is '<EOF>', while postgres COPY command can only handle a single character
##     - The last field is delimited with <startlob> and <endlob>
##   The script changes the delimiter to a single character ^ and changes the quote character back to "
## 2. Remove the MAF column from the load_variant_rc_snp_info column.  
##   As a result of the generate_VCF_loading_files.pl script, the MAF column always
##   contains an empty string. However, the datatype in the database is a bigint, so the empty
##   string must be replaced with a 0

## Change parameters here ***
## Make sure to pick a delimiter that doesn't occur in the subject file
## and adjust it in the load_variant_subject_detail.ctl as well
our $input_subject_detail = "load_variant_subject_detail.txt";
our $output_subject_detail = "load_variant_subject_detail_postgres.txt";
our $delimiter_subject_detail = "^";	

our $input_rc_snp_info = "load_variant_rc_snp_info.txt";
our $output_rc_snp_info = "load_variant_rc_snp_info_postgres.txt";
our $delimiter_rc_snp_info = "\t";	

### *****************

our(@values);

## 1. Convert subject detail info
open INPUT, "< $input_subject_detail" or die "Cannot open file: $!";
open OUTPUT, "> $output_subject_detail" or die "Cannot open file: $!";

while (<INPUT>) {
	chomp;
	@values = split(/<EOF>/);
	
	# Convert each value into a quoted one, if it contains non-numeric characters
	foreach my $value(@values) {
		# No quotes are used in the default text format
		
		#if( $value =~ m/[^0-9]/ ) {
		#	# Convert \ characters into \\
		#	$value =~ s#\\#\\\\#g;
		#	
		#	# Convert " characters into \"
		#	$value =~ s#"#\\"#g;
		#	
		#	# Surround the value with quotes
		#	$value = "\"" . $value . "\"";
		#}
		
		# Remove the <startlob> and <endlob> delimiters
		$value =~ s/<startlob>//;
		$value =~ s/<endlob>//;
	}
	
	# Output the data with proper delimiter again
	print OUTPUT join( $delimiter_subject_detail, @values ), "\n";
}

close INPUT;
close OUTPUT;

