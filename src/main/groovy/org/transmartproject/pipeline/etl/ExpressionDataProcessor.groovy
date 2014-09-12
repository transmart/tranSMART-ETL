/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 * 
 * Copyright 2012-2013 Thomson Reuters
 * 
 * This product includes software developed at Thomson Reuters
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  
 * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/

package org.transmartproject.pipeline.etl


import groovy.sql.Sql

import java.io.File;

class ExpressionDataProcessor extends DataProcessor {

	public ExpressionDataProcessor(Object conf) {
		super(conf);
	}

	@Override
	public boolean processFiles(File dir, Sql sql, Object studyInfo) {
		sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_subj_samp_map')
		sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_data')
		
		def platformList = [] as Set
		
		dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
			platformList.addAll(processMappingFile(it, sql, studyInfo))
		}
		
		platformList = platformList.toList()
		
		if (platformList.size() > 0) {
			loadPlatforms(dir, sql, platformList, studyInfo)
			
			dir.eachFileMatch(~/(?i).+_Gene_Expression_Data_[RLTZ](_GPL\d+)*\.txt/) {
				processExpressionFile(it, sql, studyInfo)
			}
		}
		else {
			throw new Exception("No platforms defined")
		}
		
		return true;
	}

	@Override
	public boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
		def studyId = studyInfo['id']
		def studyNode = studyInfo['node']
		def studyDataType = studyInfo['datatype']
		
		if (studyDataType == 'T' && !config.useT) {
			config.logger.log("Original DataType='T', but using 'Z' instead (workaround); use -t option to alter this behavior")
			studyDataType = 'Z' // temporary workaround due to a bug in Transmart
		}
		
		if (studyId && studyNode && studyDataType) {
			config.logger.log("Study ID=${studyId}; Node=${studyNode}; Data Type=${studyDataType}")
			
			if (studyInfo['runPlatformLoad']) {
				sql.call("{call i2b2_load_annotation_deapp()}")
			}
			
			sql.call("{call i2b2_process_mrna_data (?, ?, ?, null, null, '"+config.securitySymbol+"', ?, ?)}",
				[ studyId, studyNode, studyDataType, jobId, Sql.NUMERIC ]) {}
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID or Node or DataType not defined!")
			return false;
		}
		return true;
	}

	@Override
	public String getProcedureName() {
		return "I2B2_PROCESS_MRNA_DATA";
	}
	
	private List processMappingFile(File f, Sql sql, studyInfo) {
		def platformList = [] as Set
		def studyIdList = [] as Set
		
		config.logger.log("Mapping file: ${f.name}")
		
		def lineNum = 0
		
		sql.withTransaction {
			sql.withBatch(100, """\
				INSERT into lt_src_mrna_subj_samp_map (TRIAL_NAME, SITE_ID, 
					SUBJECT_ID, SAMPLE_CD, PLATFORM, TISSUE_TYPE, 
					ATTRIBUTE_1, ATTRIBUTE_2, CATEGORY_CD, SOURCE_CD) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'STD')
		""") {
				stmt ->
				
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
					// cols: 0:study_id, 1:site_id, 2:subject_id, 3:sample_cd, 4:platform, 5:tissuetype, 6:attr1, 7:attr2, 8:category_cd
					if (cols[0] && lineNum > 1) {
						if (! (cols[2] && cols[3] && cols[4] && cols[8]) )
							throw new Exception("Incorrect mapping file: mandatory columns not defined")
						
						platformList << cols[4]
						studyIdList << cols[0]	
							
						stmt.addBatch(cols)
					}
				}
			}
		}
		
		studyIdList = studyIdList.toList()
		platformList = platformList.toList()
		
		sql.commit()
		config.logger.log("Processed ${lineNum} rows")
		
		if (studyIdList.size() > 0) {
			if (studyIdList.size() > 1) {
				throw new Exception("Multiple studies in one mapping file")
			}
			else {
				def studyId = studyIdList[0]
				if (studyInfo['id'] && studyId != studyInfo['id']) {
					throw new Exception("Study ID doesn't match clinical data")
				}
				else {
					studyInfo['id'] = studyId
				}
			}
		}
		
		return platformList
	}
	
	private void loadPlatforms(File dir, Sql sql, List platformList, studyInfo) {
		platformList.each {
			platform ->
			
			sql.execute('TRUNCATE TABLE tm_lz.lt_src_deapp_annot')
			
			def row = sql.firstRow("SELECT count(*) as cnt FROM annotation_deapp WHERE gpl_id=${platform}")
			if (! row?.cnt) {
				// platform is not defined, loading
				config.logger.log("Loading platform: ${platform}")
				def f = new File(dir, "${platform}.txt")
				if (! f.exists()) throw new Exception("Platform file not found: ${f.name}")
				
				def platformTitle
				def platformOrganism
				
				row = sql.firstRow("select title, organism from de_gpl_info where platform=${platform}")
				if (!row) {
					
					config.logger.log("Fetching platform description from GEO")
					def txt = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${platform}".toURL().getText()
					
					def m = txt =~ /Title\<\/td\>\s*?\<td.*?\>(?:\[.+?\]\s*)*(.+?)\<\/td\>/
					if (m[0]) {
						platformTitle = m[0][1]
					}
					
					m = txt =~ /Organism\<\/td\>\s*?\<td.*?\>\<a.+?\>(.+?)\<\/a\>/
					if (m[0]) {
						platformOrganism = m[0][1]
					}
					
					if (platformTitle && platformOrganism) {
						sql.execute("""\
							INSERT into de_gpl_info (PLATFORM, TITLE, ORGANISM, ANNOTATION_DATE, MARKER_TYPE) 
							VALUES (?, ?, ?, sysdate, 'Gene Expression')
						""", [ platform, platformTitle, platformOrganism ])
					}
					else {
						throw new Exception("Cannot fetch platform title & organism for ${platform}")
					}
				}
				else {
					platformTitle = row.title
					platformOrganism = row.organism
				}
				
				config.logger.log("Platform: ${platformTitle} (${platformOrganism})")
				
				def lineNum = 0
				def header_mappings = [:]
				def isEmpty = true
				
				sql.withTransaction {
					sql.withBatch(500, """\
						INSERT into lt_src_deapp_annot (GPL_ID,PROBE_ID,GENE_SYMBOL,GENE_ID,ORGANISM) 
						VALUES (?, ?, ?, ?, ?)
				""") {
						stmt ->
						
						f.splitEachLine("\t") {
							cols ->
							
							lineNum++
							
							if (!cols[0] || cols[0] ==~ /\s*?#.+/ ) return // skip empty or comment lines
							
							if (!header_mappings) {
								// first line is the header if header mappings are not defined yet
								cols.eachWithIndex {
									val, idx ->
									
									if (val ==~ /(?i)(ENTREZ[\s_]*)*GENE([\s_]*ID)*/) header_mappings['entrez_gene_id'] = idx
									if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) header_mappings['gene_symbol'] = idx
									if (val ==~ /(?i)SPECIES([\s_]*SCIENTIFIC)([\s_]*NAME)/) header_mappings['species'] = idx
								}
								
								if (! header_mappings['species']) {
									// OK, trying to get species from the description
									config.logger.log(LogType.WARNING, "Species not found in the platform file, using description")
								}
								
								if (header_mappings['entrez_gene_id'] 
									&& header_mappings['gene_symbol'] 
									) {
									
									config.logger.log(LogType.DEBUG, "ENTREZ, SYMBOL, SPECIES => " + 
										"${cols[header_mappings['entrez_gene_id']]}, " +
										"${cols[header_mappings['gene_symbol']]}, " +
										"${header_mappings.containsKey('species')?cols[header_mappings['species']]:'('+platformOrganism+')'}" )
									
								}
								else {
									throw new Exception("Incorrect platform file header")
								}
							}
							else if (cols[header_mappings['entrez_gene_id']] ==~ /\d+/) {
								// line with data
								isEmpty = false
								stmt.addBatch([ 
									platform, 
									cols[0],
									cols[header_mappings['gene_symbol']],  
									cols[header_mappings['entrez_gene_id']],
									header_mappings.containsKey('species')?cols[header_mappings['species']]:platformOrganism
								])
							}
						}
					}
				}
				
				if (isEmpty) throw new Exception("Platform file doesn't contain any EntrezGene IDs")
				
				sql.commit()
				config.logger.log("Finished loading platform ${platform}, processed ${lineNum} rows")
				
				studyInfo['runPlatformLoad'] = true
			}
		}
	}
	
	private void processExpressionFile(File f, Sql sql, studyInfo) {
		config.logger.log("Processing ${f.name}")
		
		// retrieve data type
		def m = f.name =~ /(?i)Gene_Expression_Data_([RLTZ])/
		if (m[0]) {
			def dataType = m[0][1]
			if (studyInfo['datatype']) {
				if (studyInfo['datatype'] != dataType)
					throw new Exception("Multiple data types in one study are not supported")
			}
			else {
				studyInfo['datatype'] = dataType
			}
		}
		
		
		def lineNum = 0
		def header = []
		
		sql.withTransaction {
			sql.withBatch(1000, """\
				INSERT into lt_src_mrna_data (TRIAL_NAME, PROBESET, EXPR_ID, INTENSITY_VALUE) 
				VALUES (?, ?, ?, ?)
			""") {
				stmt ->
				
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
					
					if (lineNum == 1) {
						// header line
						if (cols[0] != "ID_REF") throw new Exception("Incorrect gene expression file")
						
						cols.each {
							header << it
						}
					}
					else {
						// normal data line
						config.logger.log(LogType.PROGRESS, "[${lineNum}]")
						
						cols.eachWithIndex {
							val, i ->
							
							if (i > 0) {
								// not really interested in first column here
								if (val) {
									// rows should have intensity assigned to them, otherwise not interested
									stmt.addBatch([ studyInfo['id'], cols[0], header[i], val ])
								}
							}
						}
					}
				}
			}
		}
		
		sql.commit()
		config.logger.log(LogType.PROGRESS, "")
		config.logger.log("Processed ${lineNum} rows")
	}

}
