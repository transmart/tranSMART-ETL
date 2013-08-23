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

readWordMapFile <- function(local_wordMapFile) {

  colNames   <- c("filename" , "columnNr", "oldVal"   , "newVal")
  colClasses <- c("character", "numeric" , "character", "character")

  # somehow "local_wordMapFile" always exists
  # if (exists("local_wordMapFile") == TRUE) {
  if (exists("wordMapFile") == TRUE) {
  
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
                           colClasse="character", strip.white=TRUE)
    colClasses <- 

    print(paste("Data File: ", dataFile, "read", sep=" "))

    return(dataTable)
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
	dataTable[index,columnr] <- wordMapTable$newVal[i]

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
###############################################################################
getTimestampsForColumn <- function(columnMapTable, dataTable, columnNr) {
    
    date_timestamp <- character(length(dataTable[,1]))
    dateRow <- which( columnMapTable$filename  == dataFile &
        	      columnMapTable$dataLabel == "TIMESTAMP" &
                      columnMapTable$dataLabelSource == columnMapTable$columnNr[columnNr])
    if (length(dateRow) == 1) {
	dateColumn     <- columnMapTable$columnNr[dateRow]
	date_timestamp <- dataTable[, dateColumn]
	print(paste( "    Timestamp defintion found"))
    }
    date_timestamp[which(date_timestamp == "")] <- "infinity"
    return(date_timestamp)
}

###############################################################################
###############################################################################

getUnitsForColumn <- function(columnMapTable, dataTable, columnNr) {

    units_cd <- character(length(subject_id))
    unitRow <- which( columnMapTable$filename  == dataFile &
		      columnMapTable$dataLabel == "UNITS" &
                      columnMapTable$dataLabelSource == columnMapTable$columnNr[columnNr])
    if (length(unitRow) == 1) {
	unitColumn <- columnMapTable$columnNr[unitRow]
	units_cd   <- dataTable[, unitColumn]
	print(paste( "    Units definition found"))
    }
 
    return(units_cd)
}

###############################################################################
# Here it begins.
###############################################################################

# The "default" outputFile
outputFile <- "output.tsv"

# Get arguments and assign values
args   <- commandArgs(trailingOnly = TRUE)
argmat <- sapply(strsplit(args, '='), identity)
for (i in seq.int(length=ncol(argmat))) {
      assign(argmat[1, i], argmat[2, i])
}  

if ( exists("studyID")       == FALSE |
     exists("columnMapFile") == FALSE  ) 
{
   print("Usage: Rscript load_clinical_data.R studyID=<identifier>")
   print("                                    columnMapFile=<filename>")
   print("                                    [wordMapFile=<filename>]")
   print("                                    [outputFile=<filename>]")
   stop("Please specify mandatory aruments")
}


columnMapTable <- readColumnMapFile(columnMapFile) 
wordMapTable   <- readWordMapFile(wordMapFile)

# Get the filenames with observations from "columnMap"
  dataFiles <- unique(columnMapTable$filename)

  firstWrite <- TRUE
  for (dataFile in dataFiles) {
     
      dataTable <- readDataFile(dataFile) 
      
      # Find column with "SUBJ_ID" in dataTable
      subjectIDrow = which( columnMapTable$filename  == dataFile & 
                            columnMapTable$dataLabel == "SUBJ_ID"  )
      subjectIDcolumn = columnMapTable$columnNr[subjectIDrow]      
      subject_id <- dataTable[,subjectIDcolumn]
      
      # Get studyID
      study_id <- studyID

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

      # Check if reserved word "DATA_LABEL" is used.
      # We do not support it yet (abort)
      index <- which(columnMapTable$dataLabel == "DATA_LABEL")
      if (length(index) > 0) {
	  stop("We don not support 'DATA_LABEL' columns yet, sorry for that.")
      }

      # Find the columns with a Data Label that's not a reserved word
      index <- which( columnMapTable$dataLabel != "SUBJ_ID"    & 
                      columnMapTable$dataLabel != "UNITS"      & 
                      columnMapTable$dataLabel != "MODIFIER"   & 
                      columnMapTable$dataLabel != "TIMESTAMP"  & 
                      columnMapTable$dataLabel != "OMIT"       & 
                      columnMapTable$dataLabel != "SITE_ID"    & 
                      columnMapTable$dataLabel != "VISIT_NAME" & 
                      columnMapTable$dataLabel != "DATA_LABEL"
                    )
      # Iterate over these rows in columnMapTable-file
      for ( i in index ) {
          
          # Get the label for these observations
          data_label <- columnMapTable$dataLabel[i]
          print(paste("  Observation: ", data_label))

          # Apply wordmap to this column
          dataTable <- applyWordMap(wordMapTable, dataFile, dataTable, columnMapTable$columnNr[i])

          # set MODIFIER_CD
          modifier_cd <- "@"

          # Get the UNITS if available
          units_cd <- getUnitsForColumn(columnMapTable, dataTable, i)
          
          # Get the TIMESTAMP if available
          date_timestamp <- getTimestampsForColumn(columnMapTable, dataTable, i)
        
          # Get category_cd (concept_cd) for these observations 
          category_cd <- columnMapTable$categoryCode[i]

          # Get ctrl_vocab_code for these observations
	  ctrl_vocab_code <- columnMapTable$controlledVocabCode[i] 
 
          data_value <- dataTable[, columnMapTable$columnNr[i]]
          output <- data.frame(study_id, site_id, subject_id, visit_name, 
                               data_label, modifier_cd, data_value, units_cd, date_timestamp,
                               category_cd, ctrl_vocab_code )  

          write.table(output, file=outputFile, append=!firstWrite, sep="\t", 
                      row.names=FALSE, col.names=firstWrite, quote=FALSE)
          firstWrite <- FALSE

          # Write optional MODIFIER data
          modNr <- 1
          modRow <-  which( columnMapTable$filename  == dataFile &
                            columnMapTable$dataLabel == "MODIFIER" &
                            columnMapTable$dataLabelSource == columnMapTable$columnNr[i])
          while (modNr <= length(modRow)) {
		print(paste("    MODIFIER found in column: ", modRow[modNr]))
		# Apply wordmap to this column
                dataTable <- applyWordMap(wordMapTable, dataFile, dataTable, columnMapTable$columnNr[modRow])

                # set MODIFIER_CD
                modifier_cd <- columnMapTable$controlledVocabCode[modRow]
		if (length(modifier_cd) == 0) { modifier_cd <- paste("SERIES", ":", modNr, sep="")}
                modNr <- modNr + 1

                # Get the UNITS if available
                units_cd <- getUnitsForColumn(columnMapTable, dataTable, modRow)

                # Get the TIMESTAMP if available
                date_timestamp <- getTimestampsForColumn(columnMapTable, dataTable, modRow)

                data_value <- dataTable[, columnMapTable$columnNr[modRow]]
                output <- data.frame(study_id, site_id, subject_id, visit_name,
                                     data_label, modifier_cd, data_value, units_cd, date_timestamp,
                                     category_cd, ctrl_vocab_code )

                write.table(output, file=outputFile, append=!firstWrite, sep="\t",
                            row.names=FALSE, col.names=firstWrite, quote=FALSE)

          }

      }
  }

  # To see warnings: uncomment the following line
  # warnings()

