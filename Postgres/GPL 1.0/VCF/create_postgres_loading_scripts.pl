#!/usr/bin/perl

use Cwd qw();

## This script converts the .ctl scripts within the current directory, to contain the 
## absolute path to the file to be loaded. This is required by postgres

our $scriptfile = "load_VCF_postgres.sh";

if ($#ARGV < 0) {
	print "Usage: perl create_postgres_loading_scripts.pl dbname\n";
	print "Example: perl create_postgres_loading_scripts.pl transmart\n\n";
	exit;
} else {
	our $dbname = $ARGV[0];
}

# Don't change anything below this line

our @files = (
	"load_metadata", 
	"load_variant_subject_idx",
	"load_variant_subject_summary",
	"load_variant_subject_detail",
	"load_variant_rc_snp_info"
); 

open SCRIPT, "> $scriptfile" or die "Cannot open file: $!\n";

# Loop through all files. Put the absolute path in, and add the file
# to the script to be executed
my $workingdir = Cwd::cwd() . "/";
foreach my $file (@files) {
	open CTL, "< $file.ctl";
	open NEWCTL, "> $file.postgres.ctl";
	
	while(<CTL>) {
		chomp;	
		$_ =~ s#\./#$workingdir#;
		print NEWCTL $_, "\n";
	}
	
	close NEWCTL;
	close CTL;
	 
	# Add this script to be executed from the full loading script
	print SCRIPT "psql $dbname -f '", $file, ".postgres.ctl'\n";
}

close SCRIPT;

chmod 0755, $scriptfile;
