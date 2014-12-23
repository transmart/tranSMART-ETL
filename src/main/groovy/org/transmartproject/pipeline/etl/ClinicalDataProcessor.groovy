/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 * 
 * Copyright 2012-2013 Thomson Reuters
 * 
 * This product includes software developed at Thomson Reuters
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version, along with the following terms:
 *
 * 1.	You may convey a work based on this program in accordance with section 5,
 *      provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it,
 *      in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************/

package org.transmartproject.pipeline.etl

import groovy.sql.Sql

class ClinicalDataProcessor extends DataProcessor {

	public ClinicalDataProcessor(Object conf) {
		super(conf);
	}

	@Override
	public boolean processFiles(File dir, Sql sql, studyInfo) {
		def sampleMappingRowList
		def sampleMappingCol
		def sampleMappingCheck = true
		// read mapping file first
		// then parse files that are specified there (to allow multiple files per study)
		
		config.logger.log("ClinicalData processFiles")
		config.logger.log("truncating table tm_lz.lt_src_clinical_data")

		sql.execute('TRUNCATE TABLE tm_lz.lt_src_clinical_data')
		
		config.logger.log("truncate completed")
		dir.eachFileMatch(~/(?i).+_Mapping_File\.txt/) {
			def mappings = processMappingFile(it)			
			
			if (mappings.size() <= 0) {
				config.logger.log(LogType.ERROR, "Empty mappings file!")
				throw new Exception("Empty mapping file")
			}  
			def wordMappings
			dir.eachFileMatch(~/(?i).+\.words/) {
				//procesing word mapping file
				wordMappings = processWordMappingFile(it)
			}
			List wordMappingFileNames=new ArrayList()
			//List wordMappingFileNames
			dir.eachFileMatch(~/(?i).+\.words/) {
				 wordMappingFileNames=processWordMappingFileforFileNames(it)
			}
			
			
			mappings.each {
				fName, fMappings ->
				
				config.logger.log("Processing ${fName}")
				def f = new File(dir, fName)
				if (! f.exists() ) {
					config.logger.log("File ${fName} doesn't exist!")
					throw new Exception("File doesn't exist")
				}
				/*
				 * modifed by Sony Scaria for adding sample code to work clinical data table
				 * 
				 */
				def isWordMappingExist =false
				if(wordMappingFileNames.contains(fName)){
					isWordMappingExist=true
				}				
				def sampleMappingFileName=fName
				sampleMappingFileName=sampleMappingFileName.replace(".txt","_Sample_Mapping.txt")
				def sampleMappingFile = new File(dir, sampleMappingFileName)
				config.logger.log("Checking Sample mapping file   ${sampleMappingFileName}  for ${fName}")
				if (! sampleMappingFile.exists() ) {
					config.logger.log("File ${sampleMappingFileName} doesn't exist! - ignoring Sample Mapping for the clinical data file ${fName}")
					sampleMappingCheck=false
				}
				if(sampleMappingCheck){
				sampleMappingRowList=[]
				sampleMappingFile.splitEachLine("\t") {
					def colsSamples = [''] // to support 0-index properly (we use it for empty data values)
					colsSamples.addAll(it)
					sampleMappingRowList.add(colsSamples)
				}
				}//ends sample mapping file checking				
				// modification ends here 
				
				def lineNum = 0
				//TO DO removed visit date from SQL modified by sony Scaria E0165533
				config.logger.log("sql.withTransaction INSERT into tm_lz.lt_src_clinical_data")
				sql.withTransaction {
					config.logger.log("sql.withBatch")
					sql.withBatch(100, """\
					INSERT into tm_lz.lt_src_clinical_data 
										(STUDY_ID, SITE_ID, SUBJECT_ID, VISIT_NAME, DATA_LABEL, DATA_VALUE, CATEGORY_CD,VISIT_DATE,SAMPLE_CD)
									VALUES (:study_id, :site_id, :subj_id, :visit_name, 
										:data_label, :data_value, :category_cd, :visit_date,:sample_cd)
					""") {
						stmt ->
					
						f.splitEachLine("\t") {
							
							def cols = [''] // to support 0-index properly (we use it for empty data values)
							cols.addAll(it)							
							lineNum++							
							if (lineNum < 2) return; // skipping header							
							if (cols[fMappings['STUDY_ID']]) {
								// the line shouldn't be empty								
								if (! cols[fMappings['SUBJ_ID']]) {
									throw new Exception("SUBJ_ID are not defined at line ${lineNum}")
								}
							//modified by e0165533, visit date
								//study_id : cols[fMappings['STUDY_ID']],
								def output = [									
									study_id : config.studyName,
									site_id : cols[fMappings['SITE_ID']],
									subj_id : cols[fMappings['SUBJ_ID']],
									visit_name : cols[fMappings['VISIT_NAME']],
									visit_date : cols[fMappings['VISIT_DATE']],									
									data_label : '', // DATA_LABEL
									data_value : '', // DATA_VALUE
									category_cd : '', // CATEGORY_CD
									ctrl_vocab_code : '',  // CTRL_VOCAB_CODE - unused
									sample_cd :''
								]
								def colsSamplesRow
								if(sampleMappingCheck){ 
									colsSamplesRow=sampleMappingRowList.get(lineNum-1)
								}
								
								if (fMappings['_DATA']) {
									fMappings['_DATA'].each { 
										v ->
										
										def out = output.clone()
										//adding to get the sample code
										if(sampleMappingCheck){
											def sampleID= fixColumn( colsSamplesRow[v['COLUMN']])
											out['sample_cd']=sampleID
										}
																				
										//ends
										//check for word mapping
										
										if(cols[v['COLUMN']]==null)
											{
												out['data_value'] = fixColumn( cols[v['COLUMN']])
											}
										else if(isWordMappingExist)
											{
												
												out['data_value']=getDataValue(v['COLUMN'].toString(),cols[v['COLUMN']].toString(),wordMappings,fName.toString())
											
											}
										else{
											out['data_value'] = fixColumn( cols[v['COLUMN']])
										}	
										def cat_cd = v['CATEGORY_CD']
										
										if (v['DATA_LABEL_SOURCE'] > 0) {
											// ok, the actual data label is in the referenced column
											out['data_label'] = fixColumn( cols[v['DATA_LABEL_SOURCE']] )
											// now need to modify CATEGORY_CD before proceeding
											
											// handling DATALABEL in category_cd
											if ( !cat_cd.contains('DATALABEL') ) {
												// do this only if category_cd doesn't contain DATALABEL yet
												if (v['DATA_LABEL_SOURCE_TYPE'] == 'A')
													cat_cd = (cat_cd =~ /^(.+)\+([^\+]+?)$/).replaceFirst('$1+DATALABEL+$2')
												else
													cat_cd = cat_cd + '+DATALABEL'	
											}								
												
										}		
										else {
											out['data_label'] = fixColumn(v['DATA_LABEL'])
										}
										
										// VISIT_NAME special handling; do it only when VISITNAME is not in category_cd already
										if ( ! ( cat_cd.contains('VISITNAME') || cat_cd.contains('+VISITNFST') ) ) {
											if (config.visitNameFirst) {
												cat_cd = cat_cd + '+VISITNFST'
											}
										}
										
										out['category_cd'] = fixColumn(cat_cd)
									//println output.toMapString()	
										stmt.addBatch(out) 
									}
								}
								else {
									//println output.toMapString()
									stmt.addBatch(output)
								}
							
							}
							
						}
						
					
					}
				}
				
				config.logger.log("Processed ${lineNum} rows commit")
				sql.commit()
			}
			
		}
		
		// OK, now we need to retrieve studyID & node
		def rows = sql.rows("select study_id, count(*) as cnt from tm_lz.lt_src_clinical_data group by study_id")
		def rsize = rows.size()

		config.logger.log("check results rsize ${rsize}")
		
		if (rsize > 0) {
			if (rsize == 1) {
				config.logger.log("check results rows ${rows[0]}")
				def studyId = rows[0].study_id
				if (studyId) {
					studyInfo['id'] = studyId 
				}
				else {
					config.logger.log(LogType.ERROR, "Study ID is null!")
					return false
				}
			}
			else {
				config.logger.log(LogType.ERROR, "Multiple StudyIDs are detected!")
				return false
			}
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID is not specified!")
			return false
		}
		config.logger.log("ClinicalData processFiles returning true")
		return true;
	}
	
	@Override
	public String getProcedureName() {
		// get procedure name 
		if(config.incrementalLoad.toString().equals("N"))
				return config.altClinicalProcName?:"I2B2_LOAD_CLINICAL_DATA"
		else
				return "I2B2_LOAD_CLINICAL_INC_DATA"
	}

	@Override
	public boolean runStoredProcedures(jobId, Sql sql, studyInfo) {
		def studyId = studyInfo['id']
		def studyNode = studyInfo['node']
		if (studyId && studyNode) {
			config.logger.log("Study ID=${studyId}; Node=${studyNode}; jobId=${jobId}; procedure: ${getProcedureName()}")
			
			config.logger.log("{call ${getProcedureName()}(?,?,?,?,?)}"+ [ studyId, studyNode, config.securitySymbol, 'N', jobId ])
			sql.call("{call ${getProcedureName()}(?,?,?,?,?)}", [ studyId, studyNode, config.securitySymbol, 'N', jobId ])
			config.logger.log("completed ${getProcedureName()}")
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
			return false;
		}
		
		return true;
	}
	
	private Object processMappingFile(f) {
		def mappings = [:]
		
		config.logger.log("Mapping file: ${f.name}")
		
		def lineNum = 0
		
		f.splitEachLine("\t") { 
			cols ->
			
			lineNum++
			//added by e0165533 added VISIT_DATE and ENROLL_DATE, changed Study ID
			if (cols[0] && lineNum > 1) {
				if (! mappings[cols[0]]) {
					mappings[cols[0]] = [
						STUDY_ID : 1,
						SITE_ID : 0,
						SUBJ_ID : 0,
						VISIT_NAME : 0,
						VISIT_DATE : 0,
						ENROLL_DATE :0,
						_DATA : []
							// [ { DATA_LABEL_SOURCE => 1, DATA_LABEL_SOURCE_TYPE => 'A', 
						    // DATA_LABEL => Label, CATEGORY_CD => '', COLUMN => 1 } ] - 1-based column numbers
					];
				}
				
				def curMapping = mappings[cols[0]]
				
				def dataLabel = cols[3]
				if (dataLabel != 'OMIT' && dataLabel != 'DATA_LABEL') {
					if (dataLabel == '\\') {
						// the actual data label should be taken from a specified column [4]
						def dataLabelSource = 0
						def dataLabelSourceType = ''
						
						def m = cols[4] =~ /^(\d+)(A|B){0,1}$/
						if (m.size() > 0) {
							dataLabelSource = m[0][1].toInteger()
							dataLabelSourceType = (m[0][2] in ['A', 'B'])?m[0][2]:'A'	
							//modified for sanofi
							//dataLabelSourceType='B'
						}
						
						if ( cols[1] && cols[2].toInteger() > 0 && dataLabelSource > 0) {
							curMapping['_DATA'].add([
								CATEGORY_CD : cols[1],
								COLUMN : cols[2].toInteger(),
								DATA_LABEL_SOURCE : dataLabelSource,
								DATA_LABEL_SOURCE_TYPE : dataLabelSourceType
							])
						}	
					}
					else {
						if (curMapping.containsKey(dataLabel)) {
							curMapping[dataLabel] = cols[2].toInteger()
						}
						else {
							if ( cols[1] && cols[2].toInteger() > 0 ) {
								curMapping['_DATA'].add([
									DATA_LABEL : dataLabel,
									CATEGORY_CD : cols[1],
									COLUMN : cols[2].toInteger()	
								])
							}
							else {
								config.logger.log(LogType.ERROR, "Category or column number is missing for line ${lineNum}")
								throw new Exception("Error parsing mapping file")
							}
						}
					}
				}
			}
		}
		
		return mappings
	}
	
	private String fixColumn(String s) {
		if ( s == null ) return '';
		
		def res = s.trim()
		res = (res =~ /^\"(.+)\"$/).replaceFirst('$1')
		res = res.replace('\\', '')
		res = res.replace('%', 'PCT')
		res = res.replace('*', '')
		res = res.replace('&', ' and ')
	
		return res
	}
	
	private Object processWordMappingFile( f) {
		
		
		def mappings = [:]
		
		config.logger.log("Word Mapping file: ${f.name}")
		
		def lineNum = 0
		
		f.splitEachLine("\t") {
			cols ->
			
			lineNum++
			//added by e0165533 added VISIT_DATE and ENROLL_DATE, changed Study ID
			if (lineNum > 1) {
				
					mappings[lineNum] = [
						FILE_NAME : cols[0],
						COLUMN_NUMBER : cols[1],
						ORIGINAL_VALUE : cols[2],
						NEW_VALUE : cols[3]
						];
				
				
				
			}
		}
		
		return mappings
	}
	private Object processWordMappingFileforFileNames( f) {
		
		
		List mappings = new ArrayList()
		
		config.logger.log("Word Mapping file:  ${f.name} for getting word mapping file names")
		
		def lineNum = 0
		
		f.splitEachLine("\t") {
			cols ->
			lineNum++
			//added by e0165533 added VISIT_DATE and ENROLL_DATE, changed Study ID
			if (cols[0] && lineNum > 1) {				
					mappings.add(cols[0])
				
				
			}
		}
		
		return mappings
	}
	private String getDataValue(col,val,mapping,fileName) {
		
		def returnValue=val.toString()		
	
		mapping.each {
			fName, fMappings ->
			
			if((fMappings['COLUMN_NUMBER'].toString())==col && (fMappings['ORIGINAL_VALUE'].toString())==val && (fMappings['FILE_NAME'].toString())==(fileName.toString()) ){				
				returnValue= fMappings['NEW_VALUE'].toString()				
				return returnValue
			}
			
			
		}
		
	return returnValue
}
}
