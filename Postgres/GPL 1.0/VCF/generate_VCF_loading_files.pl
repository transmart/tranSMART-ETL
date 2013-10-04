#!/usr/bin/perl

## This is the VCF shredder
## It takes a VCF file as input and create several text files to be loaded into tranSMART

if ($#ARGV < 3) {
	print "Usage: perl generate_VCF_loading_files.pl vcf_input_file datasource dataset_id ETL_user\n";
	print "Example: perl generate_VCF_loading_files.pl 54genomes_chr17_10genes.vcf CGI 54GenomesChr17 HW\n\n";
	exit;
} else {
	our $vcf_input = $ARGV[0]; 
 	our $datasource = $ARGV[1];
 	our $dataset_id = $ARGV[2];
 	our $ETL_user = $ARGV[3];
}

## Do Not change anything after this line
our $genome = "hg19";

our (@t, $rs, $clinsig, $disease, %list);

# Save the SNPs with Clinical significance and disease association information 
# Downloaded and re-processed by Haiguo Wu, Recombinant By Deloitte

# Create a map of SNPs with Clinical significance and disease 
# association information. Only the columns with SNP_ID, ClinicalSignificance 
# and VariantDisease are used. 
# N.B. The column headers in the file don't correspond to the data underneath.
#      One column header seems to be missing. 
open IN, "< hg19_snp137_clinsig_disease.txt" or die "Cannot open file: $!";
while (<IN>) {
	chomp;
	next if (/^CHR/);
	@t = split(/\t/);
	$rs = $t[2];
	$clinsig = chomp($t[12]);
	$disease = chomp($t[13]);

	next if ($clinsig eq "" && $disease eq "");
	$list{$rs} = [$clinsig, $disease];

}
close IN;

our $ETL_date = `date +FORMAT=%d-%b-%Y`;
$ETL_date =~ s/FORMAT=//;
$ETL_date =~ s/\n//;
our $depth_threshhold = 5;
our $refCount = 0;
our $altCount = 0;
our $het = 0;

# Make sure the metadata about the dataset is loaded properly.
open META, "> load_metadata.txt" or die "Cannot open file: $!";
print META "$dataset_id\t$datasource\t$ETL_user\t$ETL_date\t$genome\t$comment_file\n";
close META;

open IN, "< $vcf_input" or die "Cannot open file: $!";
open HEADER, "> vcf.header" or die "Cannot open file: $!";
open IDX, "> load_variant_subject_idx.txt" or die "Cannot open file: $!";
open DETAIL, "> load_variant_subject_detail.txt" or die "Cannot open file: $!";
open SUMMARY, "> load_variant_subject_summary.txt" or die "Cannot open file: $!";
open POPULATION_INFO, "> load_variant_population_info.txt" or die "Cannot open file: $!";
open POPULATION_DATA, "> load_variant_population_data.txt" or die "Cannot open file: $!";
# open TEMP1, "> temp1";
# open TEMP2, "> temp2";

resetNext();

our ($aaChange,$codonChange,$effect,$exonID,$class,$biotype,$gene,$impact,$transcriptID);
our $syn = 0;
our $intron = 0;
our ($chr, $pos, $rs, $ref, $alt, $qual, $filter, $info, $format, @samples);
our %infoFields;

while (<IN>) {
chomp;
	# Skip header lines, only writing them to the header file
	if (/^##/) {
		print HEADER "$_\n";
		
		# However, the info lines are used to populate the population_info table
		# The format should be 
		#   ##INFO=<ID=id,Number=number..>
		if( /^##INFO=\<(.*)\>/ ) {
			# Split the info field on ,
			@fields = split( /,/, $1 );
			my %info;
			
			# Loop through all info characteristics and store the data
			for( $j = 0; $j <= $#fields; $j++ ) {
				# Each characteristic must be of the format key=value
				( $key, $value ) = split( /=/, $fields[$j] );
				
				# If the value starts with a ", it should end with a " as well
				# if not, the string is split on a character in the middle of a 
				# textual field. This must be corrected
				if( substr( $value, 0, 1 ) eq "\"" ) {
					while( substr( $value, -1, 1 ) ne "\"" ) {
						# The value doesn't end with a ", so we should also use the next field
						$value = $value . "," . $fields[ $j + 1 ];
						$j++;
					}
					
					$value = substr( $value, 1, -1 );
				}
				
				$info{lc($key)} = $value;
			}
			
			# Only save this info field if ID is present
			if( exists( $info{"id"} ) ) {
				my $type = exists( $info{"type"} ) ? $info{"type"} : "";
				my $number = exists( $info{"number"} ) ? $info{"number"} : ".";
				print POPULATION_INFO join( "\t", 
										$dataset_id, 
										$info{"id"},
										$type,
										$number,
										exists( $info{"description"} ) ? $info{"description"} : "" ), "\n";
										
				$infoFields{$info{"id"}} = $type;
			}
		}
		
		next;
	}
	
	# Check the line for some specific modifiers, and store their values to save later on
	/SNPEFF_AMINO_ACID_CHANGE=(\w\/\w)\;/ and do {
		$aaChange = $1;
	};
	/SNPEFF_AMINO_ACID_CHANGE=(\w\/\*)\;/ and do {
		$aaChange = $1;
	};
	/SNPEFF_AMINO_ACID_CHANGE=(\*\/\w)\;/ and do {
                $aaChange = $1;
        };
	/SNPEFF_CODON_CHANGE=(\w\w\w\/\w\w\w)\;/ and do {
		$codonChange = $1;
	};
	/SNPEFF_EFFECT=(\w+)\;/ and do {
                $effect = $1;
        };
	/SNPEFF_EXON_ID=(\w+)\;/ and do {
		$exonID = $1;
	};
	/SNPEFF_FUNCTIONAL_CLASS=(\w+)\;/ and do {
		$class = $1;
	};
	/SNPEFF_GENE_BIOTYPE=(\w+)\;/ and do {
		$biotype = $1;
	};
	/SNPEFF_GENE_NAME=(\w+)\;/ and do {
		$gene = $1;
	};
	/SNPEFF_IMPACT=(\w+)\;/ and do {
		$impact = $1;
	};
	/SNPEFF_TRANSCRIPT_ID=(\w+)\;/ and do {
		$transcriptID = "";
	};

	if ($effect eq "SYNONYMOUS_CODING") {
		$syn++;
		#resetNext();
		#next;
	}
	if ($effect eq "INTRON") {
		$intron++;
		#resetNext();
		#next;
	}

	# Split the line into separate parts
	($chr, $pos, $rs, $ref, $alt, $qual, $filter, $info, $format, @samples) = split (/\t/);

	# Store a list of subject names into the _idx table 
	if ($pos eq "POS") {
		for ($i = 0; $i <= $#samples; $i++) {
			$subj = $samples[$i];
			$j = $i + 1;
			print IDX "$dataset_id\t$subj\t$j\n";
			push @subjects, $subj;
		}
		next;
	}

	## Some chromosome positions are mapped to multiple RS IDs which is OK
	## However, if a chromosme position is mapped to multiple unknown RS IDs (.), we have to exclude the repeating lines
	$location = $chr . ":" . $pos;
	if (defined $rs_saved{$location} ) {
		if ($rs eq "." && $rs_saved{$location} eq ".") {

			# print TEMP1 "$chr\t$pos\t$rs_saved{$location}\t$filter\t$rs\n";
			resetNext();
			next;
		} 
	}
	
	# Store the data for this VCF line into the _detail table
	print DETAIL join("<EOF>", $dataset_id, $chr, $pos, $rs, $ref, $alt, $qual, $filter, $info, $format),"<EOF><startlob>", join("\t", @samples), "<endlob><EOF>", "\n";

	# Store details from the info field
	my @infoData = split( /;/, $info );
	for( $j = 0; $j <= $#infoData; $j++ ) {
		# Each info field should have the format KEY=VALUE
		( $key, $value ) = split( /=/, $infoData[$j] );
		
		# Only store information about keys we know from the header
		if( !exists( $infoFields{$key} ) ) {
			print "Skipping info field $key for $chr:$pos because it is not defined in the header fields\n";
			next;
		}
		
		# To store the data later on, we need to find the correct
		# column type
		my $type = lc( $infoFields{$key} );
		
		# Handle special flag value
		if( $type eq "flag" ) {
			$value = "1";
		}
		
		# Split the column, since multiple values are to be expected
		@info = split( /,/, $value );
		
		for( $k = 0; $k <= $#info; $k++ ) {
			# Store the info value.
			$intVal = "\\N";
			$floatVal = "\\N";
			$textVal = "\\N";
		
			if( $type eq "integer" or $type eq "flag" ) {
				$intVal = $info[$k];
			} elsif( $type eq "float" ) {
				$floatVal = $info[$k];
			} elsif( $type eq "character" or $type eq "string" ) {
				$textVal = $info[$k];
			} else {
				print "Unknown data type ($type) for info field $key on $chr:$pos\n";
				next;
			} 
			 
			print POPULATION_DATA join("\t", $dataset_id, $chr, $pos, $key, $k, $intVal, $floatVal, $textVal ) .  "\n";
		}
	}


	if (defined $list{$rs} ) {
		$clinsig =  $list{$rs}[0];
		$disease =  $list{$rs}[1];
	} else {
		$clinsig = "";
		$disease = "";
	}

	if (length($ref) == 1 && length($alt) == 1) {
		$variant_type = "SNV";
	} else {
		$variant_type = "DIV";
	}
	
	# Parse the format given at this line, in order to handle the sample info properly
	# We make a hash of the index of a specific entry in the list, so for example
	#
	#		$format = gt:ad:dp
	#
	# then the hash will be:
	#
	#	{ "gt": 0, "ad": 1, "dp": 2 }
	#
	my @sampleParts = split( /:/, $format );
	my %sampleFormat;
	for( $i = 0; $i <= $#sampleParts; $i++ ) {
		$sampleFormat{@sampleParts[$i]} = $i;
	}

	# We need at least a GT column. If it is not present
	# we skip this line
	if( !exists( $sampleFormat{"GT"} ) ) {
		print "Can't import samples for line with SNP " . $rs . " (" . $pos . ") because there is no GT column present.\n";
		next;
	}
	
	# Also parse the alternatives, as it might be a list of multiples
	@alternatives = split( /\,/, $alt ); 
	
	# Loop through all samples
	for ($i = 0; $i <= $#samples; $i++) {
		unless ($samples[$i] =~ /\.\/\./) {
			# Parse the sample information, based on the format given before
			my @sampleInfo = split (/\:/, $samples[$i]);
			my $reference = false;
			
			# We are interested in the GT, AD and DP values, the others are neglected
			my $gt, $ad, $dp;
			my $ad1, $ad2;
			
			$gt = $sampleInfo[$sampleFormat{"GT"}];
			if( exists( $sampleFormat{"DP"} ) ) {
				$dp = $sampleInfo[$sampleFormat{"DP"}];
			} else {
				# If the line doesn't contain a DP value, 
				# use the sample anyway, this is achieved by
				# setting the depth to "."
				$dp = "."; 
			}
			if( exists( $sampleFormat{"AD"} ) ) {
				$ad = $sampleInfo[$sampleFormat{"AD"}];
				($ad1, $ad2) = split (/\,/, $ad);
			} else {
				# If the line doesn't contain a AD value,
				# we take half of the depth value for both
				$ad1 = $dp / 2;
				$ad2 = $ad1; 
			}
			
			$diff = abs ($ad1 + $ad2 - $depth);

			# Skip if genotype is not specified
			if( $gt eq "." ) {
   				print "Can't import sample " . $i . " for line with SNP " . $rs . " (" . $pos . "), because its genotype is not specified.\n";
   				next;
   			}

	     	# Only proceed if the read depth if larger than a set treshold
	     	# or if the read depth is not specified 
	     	if ($dp eq "." or $dp >= $depth_threshhold) {
	     		# Check whether the GT is parseable. That means it should contain a / or a |
	     		# or is numeric altogether (only one allele)
	     		if( $gt =~ m/^[0-9\.]+$/ ) {
	     			$allele = $gt;
	     			# One of the alleles is unknown
					if( $allele eq "." ) {
	     				print "Can't import sample " . $i . " for line with SNP " . $rs . " (" . $pos . "), because the genotype is unknown (" . $gt . ").\n";
	     				next;
	     			}
	     			
	     			# Compute counts for statistics
	     			if( $allele eq "0" ) {
	     				# Both alleles have the reference genotype
	     				$refCount++;
	     				$reference = true;
	     			} else {
	     				$altCount++;
	     			}
	     				
	     			# Determine the variant and variant format
	     			$variant = "";
	     			$variant_format = "";
	     			
	     			if( $allele eq "0" ) {
	     				$variant = $ref;
	     				$variant_format = "R";
	     			} else {
	     				$variant = $alternatives[$allele1 - 1];
	     				$variant_format = "V";
	     			}

					print SUMMARY join("\t", $chr, $pos, $dataset_id, $subjects[$i], $rs, $variant, $variant_format, ( $reference ? "T" : "F" ), $variant_type), "\n";	     		
	     		} elsif( $gt =~ m/[\/|]/ ) {
     				# The genotype is phased if both alleles are separated by a |, instead of a /
     				$phased = $gt =~ m/\|/; 
     				$alleleSeparator = ( $phased ? "|" : "/" );
     				
	     			($allele1, $allele2) = split( /[\/|]/, $gt );
	     			
	     			# One of the alleles is unknown
					if( $allele1 eq "." or $allele2 eq "." ) {
	     				print "Can't import sample " . $i . " for line with SNP " . $rs . " (" . $pos . "), because one (or both) of the alleles contains . (" . $gt . ").\n";
	     				next;
	     			}
	     			
					# If the alleles are different, both read depths should be larger than the treshold
					if( $allele1 != $allele2 ) {
						if ( $ad1 < $depth_threshhold || $ad2 < $depth_threshhold ) {
							next;
						}
					}
	     			
	     			# Compute counts for statistics
	     			if( $allele1 eq "0" and $allele2 eq "0" ) {
	     				# Both alleles have the reference genotype
	     				$refCount++;
	     				$reference = true;
	     			} elsif( $allele1 eq $allele2 ) {
	     				# Both alleles have the same variant
	     				$altCount++;
	     			} else {
	     				# Different alleles
	     				$het++;
	     			}
	     				
	     			# Determine the variant and variant format
	     			$variant = "";
	     			$variant_format = "";
	     			
	     			if( $allele1 eq "0" ) {
	     				$variant = $ref;
	     				$variant_format = "R";
	     			} else {
	     				$variant = $alternatives[$allele1 - 1];
	     				$variant_format = "V";
	     			}
	     			
	     			$variant = $variant . $alleleSeparator;
	     			$variant_format = $variant_format . $alleleSeparator;
	     			
	     			if( $allele2 eq "0" ) {
	     				$variant = $variant . $ref;
	     				$variant_format = $variant_format . "R";
	     			} else {
	     				$variant = $variant . $alternatives[$allele1 - 1];
	     				$variant_format = $variant_format . "V";
	     			}

					print SUMMARY join("\t", $chr, $pos, $dataset_id, $subjects[$i], $rs, $variant, $variant_format, ( $reference ? "T" : "F" ), $variant_type), "\n";
	     			
	     		} else {
     				print "Can't import sample " . $i . " for line with SNP " . $rs . " (" . $pos . "), because the GT column doesn't contain a / or a | (" . $gt . ").\n";
	     		}
			} else {
   				print "Don't import sample " . $i . " for line with SNP " . $rs . " (" . $pos . "), because the read depth (" . $dp . ") is below the treshold (" . $depth_threshhold . ").\n";
			}
	    }
	 }

	$rs_saved{$location} = $rs;
	resetNext();
}
close IN;
close OUT;
close POPULATION_INFO;
close POPULATION_DATA;

print "0/0 ref count: $refCount\n";
print "1/1 (or 2/2 etc) alt count: $altCount\n";
print "0/1 (or 2/0 etc) count: $het\n";
print "Synonymous coding change: $syn\n";
print "Within intron: $intron\n";
# print "Low depth of coverage: $lowDepth\n";

sub resetNext {
        $effect = "";
        $gt = "";
	$gene = "";
	$geneID = "";
	$strand = "";
	$maf = "";
	$chr = "";
	$pos = "";
	$rs = "";
	$ref = "";
	$alt = "";
	$variant = "";
	$variant_format = "";
	$class = "";
	$biotype = "";
	$impact = "";
	$depth = "";
	$transcriptID = "";
	$exonID = "";
	$aaChange = "";
	$codonChange = "";
}	

