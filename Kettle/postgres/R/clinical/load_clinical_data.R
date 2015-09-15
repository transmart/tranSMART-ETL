# This Rscript reads the input files for clincal data to be uploaded 
# into TranSMART (ColumnMapFile, DataFile and the optional WordMapFile).
#
# It produces one output file, which can be directly
# uploaded into landing-zone of the transmart-database.
#
# The stored procedure "i2b2_load_clincial_data" takes this data from
# the landing zone and distributes this into the transmart database.

###############################################################################
# Read the Word-Map-File
###############################################################################

options(warn=1)

readWordMapFile <- function(local_wordMapFile) {

  colNames   <- c("filename" , "columnNr", "oldVal"   , "newVal")
  colClasses <- c("character", "numeric" , "character", "character")

  if (nchar(local_wordMapFile) > 0 & file.exists(local_wordMapFile)) {

      wordMapTable <- read.csv( local_wordMapFile, header=TRUE, sep="\t",
                                col.names  = colNames,
                                colClasses = colClasses,
                                strip.white=TRUE
                              )

      print(paste("Word Map File:", local_wordMapFile, "read", sep=" "))

  } else {

      wordMapTable <- data.frame(c("", "", "", ""), col.names=colNames)  

  }

  return(wordMapTable)
}

###############################################################################
# Read the Column-Map-File
###############################################################################
readColumnMapFile <- function(columnMapFile) {

  colNames   <- c( "filename" , "categoryCode"   , "columnNr", 
                   "dataLabel", "dataLabelSource", "controlledVocabCode")

  colClasses <- c( "character", "character"      , "numeric",
                   "character", "character"      , "character")

  columnMapTable <- read.csv( columnMapFile, header=TRUE, sep="\t", 
                              col.names  = colNames,
                              colClasses = colClasses,
                              strip.white=TRUE
                            )

  print(paste("Column Map File:", columnMapFile, "read", sep=" "))

  return(columnMapTable)
}

###############################################################################
# Read the Data-File
###############################################################################
readDataFile <- function(dataFile) {

    dataTable <- read.csv( dataFile, header=TRUE, sep="\t", 
                           colClasses="character", strip.white=TRUE)

    # Do some obvious check's
    if (!"SUBJ_ID" %in% columnMapTable$dataLabel) {
        stop(paste("Mandatory dataLabel 'SUBJ_ID' not find in data-file:",
                    dataFile))
    }

    print(paste("Data File: ", dataFile, "read", sep=" "))

    return(dataTable)
}


###############################################################################
# Write Map to File
###############################################################################

writeMapToFile <- function(map, fileName) {

    write.table(map, file=fileName,
                     sep="\t",
                     row.names=FALSE, col.names=TRUE,
                     quote=FALSE,
                     na="")
}


###############################################################################
# unitsMap 
###############################################################################
getUnitsMap <- function(columnMapTable) {

  index <- which(columnMapTable$dataLabel == "UNITS")
  unitsMapTable <- as.table(matrix( c( columnMapTable$columnNr[index], 
                                       columnMapTable$dataLabelSource[index]
                                     ), 
                                    ncol=2
                                  )
                           )
  colnames(unitsMapTable) <- c("units", "values")

  return(unitsMapTable)
}

###############################################################################
# timeMap 
###############################################################################
getDateMap <- function(columnMapTable) {

  index <- which(columnMapTable$dataLabel == "DATE")
  timeMapTable <- as.table( matrix( c( columnMapTable$columnNr[index], 
                                       columnMapTable$dataLabelSource[index]
                                     ), 
                                    ncol=2
                                  )
                          )
  colnames(timeMapTable) <- c("times", "values")

  return(timeMapTable)
}


###############################################################################
# Apply the wordMap to the dataMap 
###############################################################################
applyWordMap <- function(wordMapTable, dataFile, dataTable, columnr) {

    for (i in which(wordMapTable$filename == dataFile & 
                    wordMapTable$columnNr == columnr)) {

        index <- which(dataTable[,columnr] == wordMapTable$oldVal[i])
        if (sum(index) > 0) dataTable[index,columnr] <- wordMapTable$newVal[i]

        print(paste( "    ", 
                     wordMapTable$oldVal[i], " -> ", wordMapTable$newVal[i], 
                     " (instances changed: ", length(index), ")",
                     " (row in wordmapfile: ", i, ")",
                     " (column in datafile: ", columnr, ")",
                     sep=""
                   )
             )
    }

    return(dataTable)
}

###############################################################################
# Apply the wordMap to the dataMap (for MODIFIER_CD columns)
###############################################################################
applyWordMapAs <- function(wordMapTable, dataFile, dataTable, columnr, org_columnr) {

    for (i in which(wordMapTable$filename == dataFile & 
                    wordMapTable$columnNr == org_columnr)) {

        index <- which(dataTable[,columnr] == wordMapTable$oldVal[i])
        if (sum(index) > 0) dataTable[index,columnr] <- wordMapTable$newVal[i]

        print(paste( "    ", 
                     wordMapTable$oldVal[i], " -> ", wordMapTable$newVal[i], 
                     " (instances changed*: ", length(index), ")",
                     " (row in wordmapfile: ", i, ")",
                     " (column in datafile: ", columnr, ")",
                     sep=""
                   )
             )

    }

    return(dataTable)
}

###############################################################################
###############################################################################
getTimestampsForColumn <- function(columnMapTable, dataTable, columnNr) {

    date_timestamp <- character(length(dataTable[,1]))
    dateRows       <- which( columnMapTable$filename  == dataFile &
                               columnMapTable$dataLabel == "TIMESTAMP" )

    dateRow <- c()
    for (iter in dateRows) {
        if (columnNr %in% as.numeric(unlist(strsplit(columnMapTable$dataLabelSource[iter], ',')))) {
            dateRow <- c(dateRow, iter)
        }
    }

    if (length(dateRow) == 1) {
        dateColumn     <- columnMapTable$columnNr[dateRow]
        date_timestamp <- dataTable[, dateColumn]
        print(paste( "    Timestamp definition found for column: ", columnNr))
    }
    date_timestamp[which(date_timestamp == "")] <- "infinity"
    return(date_timestamp)
}

###############################################################################
# getUnitsForColumn
###############################################################################
# Returns the units-column for "columnNr" in "dataTable" (could be empty column)

getUnitsForColumn <- function(columnMapTable, dataTable, columnNr) {

    units_cd <- character(length(dataTable[,1]))
    unitRows <- which( columnMapTable$filename  == dataFile &
                       columnMapTable$dataLabel == "UNITS"   )

    unitRow <- c()
    for (iter in unitRows) {
        if (columnNr %in% as.numeric(unlist(strsplit(columnMapTable$dataLabelSource[iter], ',')))) {
            unitRow <- c(unitRow, iter)
        }
    }

    if (length(unitRow) == 1) {
        unitColumn <- columnMapTable$columnNr[unitRow]
        units_cd   <- dataTable[, unitColumn]
        print(paste( "    Units definition found for column: ", columnNr))
    }

    return(units_cd)
}

###############################################################################
###############################################################################
# Adds derived concepts from a longitudinal variable (concept)

addLongitudinalDerivedConcepts <- function(long_out, firstWrite) {
    long_out <- long_out[long_out$data_value != "", ]
    if (is.numeric(type.convert(as.character(long_out$data_value)))) {
        long_out$data_value <- type.convert(as.character(long_out$data_value))
        addLongitudinalDerivedConceptsNumeric(long_out, firstWrite)
    } else {
        addLongitudinalDerivedConceptsFactor(long_out, firstWrite)
    }
}

addLongitudinalDerivedConceptsNumeric <- function(long_out, firstWrite) {

    # Handle the collected longitudinal data. Show derived data (min, max, mean, std-dev, ...)
    subjects <- unique(long_out$subject_id)
    for (subject_id in subjects) {
        rows       <- which( long_out$subject_id == subject_id)
        #print(long_out[rows,])
        values     <- long_out[rows, 'data_value']
        node_label <- paste(long_out[rows[1], 'category_cd'], '+', long_out[rows[1], 'data_label'], ' ...', sep='')
        # first value
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'first'
        modifier_cd     <- '@'
        data_value      <- long_out[rows[1], 'data_value']
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- long_out[rows[1], 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        firstWrite <- FALSE
        # last value
        last_row <- rows[length(rows)]
        site_id         <- long_out[last_row, 'site_id']
        visit_name      <- long_out[last_row, 'visit_name']
        data_label      <- 'last'
        modifier_cd     <- '@'
        data_value      <- long_out[last_row, 'data_value']
        units_cd        <- long_out[last_row, 'units_cd']
        date_timestamp  <- long_out[last_row, 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[last_row, 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # mean value
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'mean'
        modifier_cd     <- '@'
        data_value      <- mean(as.numeric(as.character(values)))
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Median
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'median'
        modifier_cd     <- '@'
        data_value      <- median(as.numeric(as.character(values)))
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Min value
        row             <- which(long_out$data_value == min(as.numeric(as.character(values))) &
                         long_out$subject_id == subject_id )
        site_id         <- long_out[row[1], 'site_id']
        visit_name      <- long_out[row[1], 'visit_name']
        data_label      <- 'minimum'
        modifier_cd     <- '@'
        data_value      <- long_out[row[1], 'data_value']
        units_cd        <- long_out[row[1], 'units_cd']
        date_timestamp  <- long_out[row[1], 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[row[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Max value
        row             <- which(long_out$data_value == max(as.numeric(as.character(values))) &
                                 long_out$subject_id == subject_id )
        site_id         <- long_out[row[1], 'site_id']
        visit_name      <- long_out[row[1], 'visit_name']
        data_label      <- 'maximum'
        modifier_cd     <- '@'
        data_value      <- long_out[row[1], 'data_value']
        units_cd        <- long_out[row[1], 'units_cd']
        date_timestamp  <- long_out[row[1], 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[row[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # St-dev
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'std-dev'
        modifier_cd     <- '@'
        data_value      <- sd(as.numeric(as.character(values)))
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Number
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'number'
        modifier_cd     <- '@'
        data_value      <- length(values)
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Summation
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'sum'
        modifier_cd     <- '@'
        data_value      <- sum(as.numeric(as.character(values)))
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
  }
}

addLongitudinalDerivedConceptsFactor <- function(long_out, firstWrite) {

    # Handle the collected longitudinal data. Show derived data (first, last, number, freq, factor, ...)
    subjects <- unique(long_out$subject_id)
    for (subject_id in subjects) {
        rows       <- which( long_out$subject_id == subject_id)
        values     <- long_out[rows, 'data_value']
        node_label <- paste(long_out[rows[1], 'category_cd'], '+', long_out[rows[1], 'data_label'], ' ...', sep='')
        # first value
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'first'
        modifier_cd     <- '@'
        data_value      <- long_out[rows[1], 'data_value']
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- long_out[rows[1], 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        firstWrite <- FALSE
        # last value
        last_row <- rows[length(rows)]
        site_id         <- long_out[last_row, 'site_id']
        visit_name      <- long_out[last_row, 'visit_name']
        data_label      <- 'last'
        modifier_cd     <- '@'
        data_value      <- long_out[last_row, 'data_value']
        units_cd        <- long_out[last_row, 'units_cd']
        date_timestamp  <- long_out[last_row, 'date_timestamp']
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[last_row, 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Number
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        data_label      <- 'number'
        modifier_cd     <- '@'
        data_value      <- as.character(length(values))
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        output <- data.frame(study_id, site_id, subject_id, visit_name,
                             data_label, modifier_cd, data_value, units_cd, date_timestamp,
                             category_cd, ctrl_vocab_code )
        write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                    row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        # Fraction
        site_id         <- long_out[rows[1], 'site_id']
        visit_name      <- long_out[rows[1], 'visit_name']
        modifier_cd     <- '@'
        units_cd        <- long_out[rows[1], 'units_cd']
        date_timestamp  <- ''
        category_cd     <- node_label
        ctrl_vocab_code <- long_out[rows[1], 'ctrl_vocab_code']
        for ( fact in levels(values)) {
            if (length(values[values==fact]) > 0 ) {
                data_value      <- as.character(length(values[values==fact]) / length(values))
                data_label      <- paste('fraction_', fact, sep="")
                flush.console()
                output <- data.frame(study_id, site_id, subject_id, visit_name,
                                     data_label, modifier_cd, data_value, units_cd, date_timestamp,
                                     category_cd, ctrl_vocab_code )
                write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                            row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
            }
        } 
    }
}

###############################################################################
# containsCrossDataFileColumnNumber
###############################################################################

containsCrossDataFileColumnNumbers <- function (columnNrsList) {
    # The columnNrsList argument is a list of columnNrs
    # The columnNrs element (string) could be a comma seperated list of columnNr's
    # Each columnNr could be a pure local column number (integer) for a column in the file with name filename.
    # Or columnNr is a composition of (another) filename and a column number (<otherfilename>;<integer>) for a column in the file with name <otherfilename>.

    if ( length(columnNrsList) == 0 ) return (FALSE)

    for (columnNrs in columnNrsList)
    {
        if ( !is.na(columnNrs) && nchar(columnNrs)>0 )
        {
            columnNrList <- strsplit(columnNrs,",")[[1]]

            for (columnNr in columnNrList) {
                filecolumnsplit <- strsplit(columnNr,";")[[1]]

                if (length(filecolumnsplit) == 2) {
                    # columnNr probably is a cross data file column number
                    return(TRUE)
                }
            }
        }
    }
    return(FALSE)
}

###############################################################################
# getGlobalColumnNumber
###############################################################################

getGlobalColumnNumber <- function (filename,columnNrs,columnOffsetMap,subjIdColumn) {
    # The columnNrs argument could be a comma seperated list of columnNr's
    # Each columnNr could be a pure local column number (integer) for a column in the file with name filename.
    # Or columnNr is a composition of (another) filename and a column number (<otherfilename>;<integer>) for a column in the file with name <otherfilename>.
    # columnOffsetMap contains a map/dictionary with column offsets in the global data file for each of the datafiles.
    # subjIdColumn contains a map/dictionary with column nr of the SUBJ_ID column in the original data file for each of the datafiles.

    if (is.na(columnNrs) || nchar(columnNrs)==0) return(columnNrs)

    columnNrList <- strsplit(columnNrs,",")[[1]]

    globalcolumnnrs <- ""

    for (columnNr in columnNrList) {
        filecolumnsplit <- strsplit(columnNr,";")[[1]]
        if (length(filecolumnsplit) == 2) {
            # columnNr probably is a referral local column number already
            fileColumnNumber <- columnNr
        } else {
            fileColumnNumber <- paste(filename,columnNr,sep=';')
        }
        globalcolumnnr <- getGlobalColumnNumberFromFileColumnNumber(fileColumnNumber, columnOffsetMap, subjIdColumn)
        if (nchar(globalcolumnnrs) > 0) {
            globalcolumnnrs <- paste(globalcolumnnrs, globalcolumnnr, sep=",")
        } else {
            globalcolumnnrs <- globalcolumnnr
        }
    }
    return(globalcolumnnrs)
}

###############################################################################
# getGlobalColumnNumberFromFileColumnNumber
###############################################################################

getGlobalColumnNumberFromFileColumnNumber <- function (fileColumnNumber, columnOffsetMap, subjIdColumn) {

    # fileColumnNumber is a composition of a filename and a column number (<filename>;<integer>) for a column in the file with name <filename>.
    # columnOffsetMap contains a map/dictionary with column offsets in the global data file for each of the datafiles.
    # subjIdColumn contains a map/dictionary with column nr of the SUBJ_ID column in the original data file for each of the datafiles.

    filecolumnsplit <- strsplit(fileColumnNumber,";")[[1]]

    if (length(filecolumnsplit) != 2) stop( paste("Invalid fileColumnNumber (",fileColumnNumber,") encoutered",sep=''))

    filename <- filecolumnsplit[1]
    localcolumnnr <- as.integer(filecolumnsplit[2])

    if (is.na(localcolumnnr)) stop( paste("Invalid fileColumnNumber (",fileColumnNumber,") encoutered",sep=''))

    localsubjidcolumn <- subjIdColumn[[filename]]

    # All local SUBJ_ID columns are replaced with a single SUBJ_ID column in the global data file (column nr 1).
    # So local column nrs <  localsubjidcolumn: are ok w.r.t. offset
    #    local column nrs == localsubjidcolumn: replace with global column nr 1
    #    local column nrs >  localsubjidcolumn: are 1 to large w.r.t. offset
    if (localcolumnnr == localsubjidcolumn) {
         globalcolumnnr <- 1
    } else if (localcolumnnr < localsubjidcolumn) {
        globalcolumnnr <- columnOffsetMap[[filename]]+localcolumnnr
    } else {
        globalcolumnnr <- columnOffsetMap[[filename]]+localcolumnnr - 1
    }

    return(as.integer(globalcolumnnr))
}

###############################################################################
# convertToSingleDataTable
###############################################################################

convertToSingleDataTable <- function (outputfilenames, columnMapTable, wordMapTable, doMerge) {

    # A single data file is by default built in block diagonal form.
    # If doMerge is set (TRUE), the single data file is built by merging the data files by subject_id, resulting in a single data file with at most one data row per subject_id
    # (but only if each of the data files contains a single data row per subject)

    # Get the filenames with observations from "columnMap"
    dataFiles <- unique(columnMapTable$filename)

    singleDataTable <- data.frame()
    singleDataTableSubjIdColumnName <- character()
    singleDataTableSubjIdColumnIndex <- integer()
    dataFileColumnOffset <- list()
    dataFileSubjIdColumn <- list()

    for (dataFile in dataFiles) {

        dataTable <- readDataFile(dataFile)

        # Find column with "SUBJ_ID" in dataTable
        subjectIdRow = which( columnMapTable$filename  == dataFile & 
                              columnMapTable$dataLabel == "SUBJ_ID"  )

        if (length(subjectIdRow)==0 || subjectIdRow < 1) stop(paste("No SUBJ_ID column found in column mapping file (",columnMapFile,") for file: ",dataFile,sep=""))

        subjectIdColumn = columnMapTable$columnNr[subjectIdRow]      
        subjects <- dataTable[,subjectIdColumn]

        # subject can not be NA or empty string
        if (any(is.na(subjects)) || any(subjects=='') )
        {
            stop("Invalid subject ids encountered")
        }

        # subject should occur in the list only once
        if ( doMerge && length(unique(subjects)) != length(subjects) )
        {
            stop("Duplicate subject ids encountered")
        }

        firstDataFile <- ncol(singleDataTable)==0

        if(firstDataFile)
        {   # Remember original column name of subject_ids in first data file
            singleDataTableSubjIdColumnName <- colnames(dataTable)[subjectIdColumn]
        }

        if (length(dataFiles) > 1) {
            # Update column names to make them different for each of the data files; it will prevent name clashes during merging
            iloop <- match(dataFile,dataFiles)
            colnames(dataTable) <- paste(colnames(dataTable),".",iloop,sep="")
        }

        # Register the column offset in the singleDataTable for the columns from each dataFile.
        dataFileColumnOffset[[dataFile]] <- ncol(singleDataTable)
        # Register the column nr of the SUBJ_ID column for each dataFile.
        dataFileSubjIdColumn[[dataFile]] <- subjectIdColumn

        if(firstDataFile)
        {
            # Correct the column offset in the singleDataTable for the columns of first dataFile.
            dataFileColumnOffset[[dataFile]] <- 1

            singleDataTable <- dataTable
            colnames(singleDataTable)[subjectIdColumn] <- singleDataTableSubjIdColumnName

        } else {

            if (doMerge)
            {
                singleDataTable <- merge(singleDataTable, dataTable, by.x=1, by.y=subjectIdColumn, all=TRUE)

            } else {

                # Construct next block in block diagonal matrix

                #remove subject_id column from dataTable
                dataTableSubjectIdColname <- colnames(dataTable)[subjectIdColumn]
                dataTable <- dataTable[,!(colnames(dataTable) %in% dataTableSubjectIdColname)]

                singleDataTableColumns <- colnames(singleDataTable)
                dataTableColumns <- colnames(dataTable)

                singleDataTable[,dataTableColumns] <- NA
                dataTable[,singleDataTableColumns] <- NA
                dataTable[,singleDataTableSubjIdColumnName] <- subjects

                singleDataTable <- rbind(singleDataTable, dataTable)
            }
        }
    }

    # Remove all SUBJ_ID rows, except the first one
    rowsToKeep <- columnMapTable$dataLabel != "SUBJ_ID" # all non SUBJ_ID rows have value TRUE
    # Determine first SUBJ_ID row
    firstSubjIdRow <- match("SUBJ_ID", columnMapTable$dataLabel)
    # Keep also this first SUBJ_ID row
    rowsToKeep[firstSubjIdRow] <- TRUE
    # Remove rows
    columnMapTable <- columnMapTable[rowsToKeep,]

    # Write map to file already takes care of replacing NA values with empty cells
    # singleDataTable[is.na(singleDataTable)] <- ""

    singleDataFileName <- outputfilenames[["data"]]
    writeMapToFile(singleDataTable,singleDataFileName)

    # Update the column numbers in the ColumnMapFile and the WordMapFile to corresponding column numbers in singleDataTable
    # Local column numbers need to be changed to global column numbers.
    # Pure local column numbers (e.g. 4) refer to a column (4) in the data file mentioned on the same line in the mapping file.
    # Referral local column numbers (e.g. datafile-followup-003.txt;4) refer to a column (4) in the datafile mentioned before the semicolon (datafile-followup-003.txt).

    columnMapTable[,'columnNr'] <- apply(columnMapTable[,c('filename','columnNr')], 1, function(x) { getGlobalColumnNumber(x[1],x[2],dataFileColumnOffset,dataFileSubjIdColumn) })
    columnMapTable[,'dataLabelSource'] <- apply(columnMapTable[,c('filename','dataLabelSource')], 1, function(x) { getGlobalColumnNumber(x[1],x[2],dataFileColumnOffset,dataFileSubjIdColumn) })
    columnMapTable[,'filename'] <- singleDataFileName

    singleDataColumnMapFileName <- outputfilenames[["columnmap"]]
    writeMapToFile(columnMapTable,singleDataColumnMapFileName)

    if ( nrow(wordMapTable) > 0 ) {
        wordMapTable[,'columnNr'] <- apply(wordMapTable[,c('filename','columnNr')], 1, function(x) { getGlobalColumnNumber(x[1],x[2],dataFileColumnOffset,dataFileSubjIdColumn) })
        wordMapTable[,'filename'] <- singleDataFileName
    }

    singleDataWordMapFileName <- outputfilenames[["wordmap"]]
    writeMapToFile(wordMapTable,singleDataWordMapFileName)
}

###############################################################################
# Here it begins.
###############################################################################

# The "default" outputFile
outputFile <- "output.tsv"

# Get arguments and assign values
args   <- commandArgs(trailingOnly = TRUE)
argmat <- sapply(strsplit(args, '='), identity)

if (length(argmat)==0) {
    studyId <- "TEST"
    columnMapFile <- "TEST_columns_map.txt"
    wordMapFile <- "TEST_word_map.txt"
} else {
    for (i in seq.int(length=ncol(argmat)))
    {
          assign(argmat[1, i], argmat[2, i])
    }
}

# The preferred argument name is studyId
# The use of studyID is deprecated, but still supported.
if ( ( !exists("studyId") & !exists("studyID") ) || !exists("columnMapFile")  ) 
{
   print("Usage: Rscript load_clinical_data.R studyId=<identifier>")
   print("                                    columnMapFile=<filename>")
   print("                                    [wordMapFile=<filename>]")
   print("                                    [outputFile=<filename>]")
   stop("Please specify mandatory arguments")
}

if ( exists("studyId") & exists("studyID") )
{
   print("Usage: Rscript load_clinical_data.R studyId=<identifier>")
   print("                                    columnMapFile=<filename>")
   print("                                    [wordMapFile=<filename>]")
   print("                                    [outputFile=<filename>]")
   stop("Please use studyId argument solely")
}

if ( exists("studyID") ) { studyId <- studyID }
if ( !exists("wordMapFile") ) { wordMapFile <- "" }

columnMapTable <- readColumnMapFile(columnMapFile) 
wordMapTable   <- readWordMapFile(wordMapFile)

# Get the filenames with observations from "columnMap"
dataFiles <- unique(columnMapTable$filename)

collapseDataIntoSingleDataFile <- FALSE
doMerge <- FALSE

if (length(dataFiles) > 1) {

    # Cross data file column references in general require that all data files are merged by subject_id
    # into a single data file. This requires that no data file contains multiple rows for the same patient.
    # The use of cross data file references in a REPEAT construct, doesn't require that the data files are merged.
    # It suffices to build the single data file in a block diagonal fashion, which approach doesn't require
    # that data files contain a single data row for each subject_id.
    # If no cross data file column references are used, processing the block diagonal single data file
    # will give the same result as processing each data file seperately.

    # Only need to take action if cross data file reference are found (by default collapsing data is false)
    crossDataFileReferences <- containsCrossDataFileColumnNumbers(columnMapTable$dataLabelSource)

    if ( crossDataFileReferences )
    {
        collapseDataIntoSingleDataFile <- TRUE

        # Only if cross data file column references are made for non-REPEAT constructs, the data files need to be merged
        # Otherwise the block diagonal approach suffices (which is the default).

        nonRepeatColumnNumbers <- columnMapTable[columnMapTable$dataLabel!='REPEAT','dataLabelSource']
        crossDataFileReferencesOtherThenForRepeat <- containsCrossDataFileColumnNumbers(nonRepeatColumnNumbers)

        if ( crossDataFileReferencesOtherThenForRepeat )
        {
            doMerge <- TRUE
        }
    }
}

if ( collapseDataIntoSingleDataFile )
{
    # multiple data files; convert to a single data file and update column nr's in column map file and word map file.

    singleDataFilename           <- paste(studyId,"_SingleDataFile.txt",sep="")
    singleDataColumnMapFilename  <- paste(studyId,"_SingleDataFile_ColumnMapping.txt",sep="")
    singledataWordMapFilename    <- paste(studyId,"_SingleDataFile_WordMapping.txt",sep="")

    outputfilenames <- list()
    outputfilenames[["data"]]      <- singleDataFilename
    outputfilenames[["columnmap"]] <- singleDataColumnMapFilename
    outputfilenames[["wordmap"]]   <- singledataWordMapFilename

    convertToSingleDataTable(outputfilenames, columnMapTable, wordMapTable, doMerge=doMerge)

    columnMapTable <- readColumnMapFile(singleDataColumnMapFilename)
    wordMapTable   <- readWordMapFile(singledataWordMapFilename)

    # Get the filenames with observations from "columnMap"
    dataFiles <- unique(columnMapTable$filename)

    # assert length(dataFiles) == 1
    # assert dataFiles[1] == singleDataFilename
    if (length(dataFiles) != 1)             stop(paste("Error converting multiple data files to a single data file; expected exactly 1 data file; found ",length(dataFiles)))
    if (dataFiles[1] != singleDataFilename) stop(paste("Error converting multiple data files to a single data file; expected single data file name to be",singleDataFilename,"; found",dataFiles[1]))
}

firstWrite <- TRUE
for (dataFile in dataFiles) {

    dataTable <- readDataFile(dataFile)
    nRowsDataTable <- length(dataTable[,1]) 

    # Find column with "SUBJ_ID" in dataTable
    subjectIDrow = which( columnMapTable$filename  == dataFile & 
                          columnMapTable$dataLabel == "SUBJ_ID"  )
    subjectIDcolumn = columnMapTable$columnNr[subjectIDrow]      
    subject_id <- dataTable[,subjectIDcolumn]

    # Get studyId
    study_id <- studyId

    # Get siteIDs
    site_id <- character(length(subject_id))
    siteIDrow = which(columnMapTable$filename  == dataFile & 
                      columnMapTable$dataLabel == "SITE_ID" )

    if (length(siteIDrow) == 1) {
        siteIDcolumn <- columnMapTable$columnNr[siteIDrow]
        site_id      <- dataTable[, siteIDcolumn]
    }

    # Get visitNames
    visit_name = character(length(subject_id))
    visitNameRow = which(columnMapTable$filename  == dataFile &
                         columnMapTable$dataLabel == "VISIT_NAME" )

    if (length(visitNameRow) == 1) {
        visitNameColumn <- columnMapTable$columnNr[visitNameRow]
        visit_name      <- dataTable[, visitNameColumn]
    }

    # Find the columns with a Data Label that's not a reserved word
    index <- which( columnMapTable$dataLabel != "SUBJ_ID"    & 
                    columnMapTable$dataLabel != "UNITS"      & 
                    columnMapTable$dataLabel != "REPEAT"     & 
                    columnMapTable$dataLabel != "TIMESTAMP"  & 
                    columnMapTable$dataLabel != "OMIT"       & 
                    columnMapTable$dataLabel != "SITE_ID"    & 
                    columnMapTable$dataLabel != "VISIT_NAME" & 
                    columnMapTable$dataLabel != "DATA_LABEL" &
                    columnMapTable$filename  == dataFile
                  )
    # Iterate over these rows in columnMapTable-file
    for ( i in index ) {

        # Get category_cd (concept_cd) for these observations 
        category_cd <- rep(columnMapTable$categoryCode[i], nRowsDataTable)

        # Get the label for these observations
        data_label <- rep(columnMapTable$dataLabel[i], nRowsDataTable)
        print(paste("  Observation: ", category_cd[1], " + ", data_label[1]))

        # Apply wordmap to this column
        dataTable <- applyWordMap(wordMapTable, dataFile, dataTable, columnMapTable$columnNr[i])

        # set MODIFIER_CD
        modifier_cd <- "@"

        # Get the UNITS if available
        units_cd <- getUnitsForColumn(columnMapTable, dataTable, columnMapTable$columnNr[i])

        # Get the TIMESTAMP if available
        date_timestamp <- getTimestampsForColumn(columnMapTable, dataTable, columnMapTable$columnNr[i])

        # Get ctrl_vocab_code for these observations
        ctrl_vocab_code <- columnMapTable$controlledVocabCode[i] 

        # Handle references to DATA LABEL columns (clumbsy, who can do better?)
        if (grepl("^[\\]"           , columnMapTable$dataLabel[i])       &
            grepl("[0-9]+(,[0-9]+)*", columnMapTable$dataLabelSource[i])   ) 
        {
            if (grepl("%", category_cd[1]) | grepl("%", data_label[1]))
            {
                dl_columns <- strsplit(columnMapTable$dataLabelSource[i], ',')
                count <- 1
                for (pos in as.numeric(unlist(dl_columns))) 
                {
                    for (row in c(1:nRowsDataTable)) 
                    {
                        category_cd[row] <- gsub(paste("%", count, sep=""), 
                                                 dataTable[row,as.numeric(pos)], 
                                                 category_cd[row] )
                    }

                    for (row in c(1:nRowsDataTable)) 
                    {
                        data_label[row] <- gsub(paste("%", count, sep=""), 
                                                dataTable[row,as.numeric(pos)], 
                                                data_label[row] )
                    }
                    count <- count + 1
                }

            } else {

                # add to end of data_label (backwards compatibility)
                data_label <- paste(data_label, "\\", dataTable[,as.numeric(columnMapTable$dataLabelSource[i])], sep="")
                print("    Depricated functionality.")
                print("    Please specify '%1' in 'catogory_code' or 'data_label' to insert label column next time")
            }
        }

        # Are there REPEATS for this concept? If there are REPEATS we do not write the original
        # data we only write derived data
        repRow <- which( columnMapTable$filename  == dataFile &
                         columnMapTable$dataLabel == "REPEAT" &
                         columnMapTable$dataLabelSource == columnMapTable$columnNr[i])

        # Write Concept
        data_value <- dataTable[, columnMapTable$columnNr[i]]
        index <- which(data_value != "")
        tmp_output <- data.frame(study_id, site_id, subject_id, visit_name, 
                                 data_label, modifier_cd, data_value, units_cd, date_timestamp,
                                 category_cd, ctrl_vocab_code )  
        output <- tmp_output[index, ]
        #write.table(output, file=outputFile, append=!firstWrite, sep="\t",
        #          row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
        #firstWrite <- FALSE
        # now write_table should be done. We only write it if no modifiers exist for this concept

        # Write optional REPEAT data
        repeat_out <- output   # collect longitudinal data (dataLabel == REPEAT)
        repNr  <- 1
        for (repNr in repRow) {
            print(paste("    REPEAT found in column: ", columnMapTable$columnNr[repNr]))
            # Apply wordmap to this column
            dataTable <- applyWordMap(wordMapTable, dataFile, dataTable, columnMapTable$columnNr[repNr])
            dataTable <- applyWordMapAs(wordMapTable, dataFile, dataTable, 
                                        columnMapTable$columnNr[repNr], columnMapTable$columnNr[i])

            # set MODIFIER_CD
            modifier_cd <- columnMapTable$controlledVocabCode[repNr]
            if (nchar(modifier_cd) == 0) { modifier_cd <- paste("SERIES", ":", repNr, sep="")}

            # Get the UNITS if available
            units_cd <- getUnitsForColumn(columnMapTable, dataTable, columnMapTable$columnNr[repNr])

            # Get the TIMESTAMP if available
            date_timestamp <- getTimestampsForColumn(columnMapTable, dataTable, columnMapTable$columnNr[repNr])

            data_value <- dataTable[, columnMapTable$columnNr[repNr]]
            index <- which(data_value != "") 
            tmp_output <- data.frame(study_id, site_id, subject_id, visit_name,
                                     data_label, modifier_cd, data_value, units_cd, date_timestamp,
                                     category_cd, ctrl_vocab_code )
            output <- tmp_output[index,]
            #write.table(output, file=outputFile, append=!firstWrite, sep="\t",
            #            row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
            #firstWrite <- FALSE

            repeat_out <- rbind(repeat_out, output)   # Add to already collected longitudinal data
        }

        # Check for presence of repeating observations (per concept)
        category_cds <- unique(repeat_out$category_cd)
        data_labels <- unique(repeat_out$data_label)
        # Handle per concept
        for ( cat_cd in category_cds ) {
            for ( dt_lbl in data_labels ) {
                rows <- which( repeat_out$category_cd == cat_cd & repeat_out$data_label == dt_lbl )
                # filter on concepts
                perconcept_out <- repeat_out[rows,]
                # determine number of distinct subjects
                subjects <- unique(perconcept_out$subject_id)
                if ( nrow(perconcept_out) > length(subjects) ) {
                    # At least one subject has more then one observation for this concept
                    # Process these as longitudinal observations
                    addLongitudinalDerivedConcepts(perconcept_out, firstWrite)
                    firstWrite <- FALSE 
                } else {
                    write.table(perconcept_out, file=outputFile, append=!firstWrite, sep="\t",
                                row.names=FALSE, col.names=firstWrite, quote=FALSE, na="")
                    firstWrite <- FALSE
                }
            }
        }
    }
}

# To see warnings: uncomment the following line
warnings()
