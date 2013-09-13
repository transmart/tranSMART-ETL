# Check input parameters
if [ $# -lt 4 ]
  then
    echo "No or invalid arguments supplied."
    echo "Usage: ./load_mapping_data.sh subject_sample_mapping_file source fullpath(separated by +) dbname"
    echo "Example: ./load_mapping_data.sh subject_sample.txt TEST \"Public Studies+GSE8581+Exome Sequencing\" transmart"
    exit 1
fi

perl generate_VCF_mapping_files.pl "$1" "$2" "$3" "$4"

./load_mapping_tables.sh
