# Check input parameters
if [ $# -lt 5 ]
  then
    echo "No or invalid arguments supplied."
    echo "Usage: ./load_vcf_data.sh vcf_input_file datasource dataset_id ETL_user dbname"
    echo "Example: ./load_vcf_data.sh 54genomes_chr17_10genes.vcf CGI 54GenomesChr17 RH transmart"
    exit 1
fi

perl generate_VCF_loading_files.pl $1 $2 $3 $4
perl convert_VCF_loading_files_to_postgres.pl 
perl create_postgres_loading_scripts.pl $5

./load_VCF_postgres.sh
