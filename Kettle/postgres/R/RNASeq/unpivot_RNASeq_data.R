
###############################################################################
# Read the RNA-seq-File
###############################################################################

readRNASeqFile <- function(local_RNASeqFile) {

    RNASeqTable <- read.csv( local_RNASeqFile, sep="\t", strip.white=TRUE, check.names=FALSE )

    print(paste("RNA Seq File:", local_RNASeqFile, "read", sep=" "))
  
    return(RNASeqTable)
}

###############################################################################
# Read the Subject Sample map file
###############################################################################
readSubjectSampleMapFile <- function(local_SubjectSampleFile) {

    colNames <- c( "STUDY_ID", "SITE_ID", "SUBJECT_ID", "SAMPLE_ID",
                   "PLATFORM", "ATTR1", "ATTR2", "CATEGORY_CD", "SOURCE_CD" )

    subjectSampleTable <- read.csv( local_SubjectSampleFile, 
                                    header=TRUE, 
                                    sep='\t',
                                    col.names = colNames,
                                    strip.white=TRUE
                                  )

    return(subjectSampleTable)

}


###############################################################################
###############################################################################
platformOUT <- "platform.tsv"
dataOUT     <- "data.tsv"


# Get arguments and assign values
args   <- commandArgs(trailingOnly = TRUE)
argmat <- sapply(strsplit(args, '='), identity)
for (i in seq.int(length=ncol(argmat))) {
      assign(argmat[1, i], argmat[2, i])
}  

if ( !exists("studyID") | !exists("RNASeqFile")  ) 
{
   print("Usage: Rscript unpivot_RNASeq_data.R studyID=<identifier>")
   print("                                     RNASeqFile=<filename>")
##   print("                                     platformID=<identifier>")
##   print("                                     subjectSampleMapFile=<filename>")
   stop("unpivot_RNASeq_data.R: Please specify mandatory arguments")
}

RNASeqTable        <- readRNASeqFile(RNASeqFile)

##subjectSampleTable <- readSubjectSampleMapFile(subjectSampleMapFile)

##if ( exists("platformID") ) 
##{
##
##    # Write the platform file
##    print(paste("Create platform-file: ", platformOUT, sep=""))
##
##    GPL_ID      <- paste(platformID)
##    REGION_NAME <- rownames(RNASeqTable)
##    CHROMOSOME  <- ""
##    BEGIN_BP    <- ""
##    END_BP      <- "" 
##    NUM_PROBES  <- ""
##    CYTOBAND    <- ""
##    GENE_SYMBOL <- rownames(RNASeqTable)
##    GENE_ID     <- ""
##    ORGANISM    <- "Homo Sapiens"
##
##    output <- data.frame(GPL_ID, REGION_NAME, CHROMOSOME, BEGIN_BP, END_BP,
##                         NUM_PROBES, CYTOBAND, GENE_SYMBOL, GENE_ID, ORGANISM)
##
##    write.table(output, file=platformOUT, sep='\t', col.names=TRUE, row.names=FALSE, quote=FALSE)
##}

# Write the unpivoted data file.
print(paste("Info unpivot_RNASEQ_data.R: Create unpivoted data-file: ", dataOUT, sep=""))

columnnames <- colnames(RNASeqTable)      

## Check if input file already has been unpivoted
if (length(columnnames) == 5) {
    expectedcolumnclasses <- c("factor", "factor", "factor", "integer")
    columnclasses <- sapply(RNASeqTable[,1:4], class)
    if ( all(columnclasses == expectedcolumnclasses) ) {
        file.copy(RNASeqFile, dataOUT, overwrite=TRUE)
        print("Info unpivot_RNASEQ_data.R: Input file looks like a file which already has been unpivoted and is therefore copied to the output unmodified")
        quit(status=0)
    }
}

## Investigate what is provided
readcountsProvided=FALSE
normalizedreadcountsProvided=FALSE

## Calculate the number of column names 
ncolwithdot <- length(grep("\\.", columnnames))
if (ncolwithdot == 0) {

    ## No column names contain a dot; assume readcounts are provided and column names are the sample names
    readcountcolumnnames <- columnnames
    samplenames <- columnnames
    readcountsProvided=TRUE
    
} else if (ncolwithdot == length(columnnames)) {

    ## All column names contain a dot; assume it is built from <variablename>.<sampleid>.
    readcountcolumnnames = grep("^readcount\\.",columnnames,value=TRUE)
    normalizedreadcountcolumnnames = grep("^normalizedreadcount\\.",columnnames,value=TRUE)

    if ( ncolwithdot != (length(readcountcolumnnames)+length(normalizedreadcountcolumnnames)) ) {
        stop("unpivot_RNASeq_data.R: Not all column names contain either readcount or normalizedreadcount")
    }

    if ( length(normalizedreadcountcolumnnames) == 0 ) {
    
        # Only readcounts provided
        samplenames <- gsub("^.*\\.","",readcountcolumnnames)
        readcountsProvided=TRUE
        
    } else if ( length(readcountcolumnnames) == 0 ) {
    
        # Only normalizedreadcounts provided
        samplenames <- gsub("^.*\\.","",normalizedreadcountcolumnnames)
        normalizedreadcountsProvided=TRUE
        
    } else {

        if ( length(readcountcolumnnames) != length(normalizedreadcountcolumnnames) ) {
            stop("unpivot_RNASeq_data.R: Number of samples for which readcounts are provided differs from the number of samples for which normalizedreadcounts are provided")
        }
        
        # Check if readcounts and normalizedreadcounts are provided for the same samples
        readcountsamplenames <- gsub("^.*\\.","",readcountcolumnnames)
        normalizedreadcountsamplenames <- gsub("^.*\\.","",normalizedreadcountcolumnnames)
        if (!all(sort(readcountsamplenames) == sort(normalizedreadcountsamplenames))) {
           stop("List of sample names for readcounts differs from normalizedreadcounts")       
        }

        samplenames <- readcountsamplenames
        readcountsProvided=TRUE
        normalizedreadcountsProvided=TRUE
    }

} else {

    ## Invalid header
    stop("unpivot_RNASeq_data.R: Input file with RNASeq data contains an invalid header")

}

# Clean NA cells in RNASeqTable
RNASeqTable[is.na(RNASeqTable)] <- ""
trial_name  <- studyID
region_name <- rownames(RNASeqTable)
readcount   <- ""

firstWrite <- TRUE
i <- 1

while (i <= length(samplenames)) {

    expr_id <- samplenames[i]
    
    readcount           <- ""
    normalizedreadcount <- ""
    
    if (readcountsProvided) {
        readcountcolumnname=readcountcolumnnames[i]
        readcount <- RNASeqTable[,readcountcolumnname]
    }
    if (normalizedreadcountsProvided) {
        normalizedreadcountcolumnname=paste("normalizedreadcount",samplenames[i],sep='.')
        normalizedreadcount <- RNASeqTable[,normalizedreadcountcolumnname]
    }
    output <- data.frame(trial_name, region_name, expr_id, readcount, normalizedreadcount)
    write.table(output, file=dataOUT, append=!firstWrite, sep="\t", 
                row.names=FALSE, col.names=firstWrite, quote=FALSE)
    firstWrite <- FALSE
    i = i + 1
}

# To see warnings: uncomment the following line
# warnings()

