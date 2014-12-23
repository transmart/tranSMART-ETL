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
 * at your option) any later version, along with the following terms:
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

import java.io.File;

class RBMDataProcessor extends DataProcessor {

	public RBMDataProcessor(Object conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean processFiles(File dir, Sql sql, Object studyInfo) {
		sql.execute('TRUNCATE TABLE stg_rbm_antigen_gene')
		sql.execute('TRUNCATE TABLE stg_subject_rbm_data')
		
		// process antigen mappings first
		dir.eachFileMatch(~/(?i).+_Antigen_Gene_Mapping(_File)*\.txt/) {
			processMappingFile(it, sql, studyInfo)
		}
		
		// process RBM data
		dir.eachFileMatch(~/(?i).+_RBM_Data\.txt/) {
			processRBMFile(it, sql, studyInfo)
		}
		
		return true
	}
	
	private void processMappingFile(File f, Sql sql, Object studyInfo) {
		def lineNum = 0
		
		config.logger.log("Processing Antigen mapping file ${f.name}")
		
		sql.withTransaction {
			sql.withBatch(100, """\
					INSERT into stg_rbm_antigen_gene (antigen_name, gene_symbol, gene_id)
					VALUES (?, ?, ?)
					""") {
				stmt ->
			
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
					
					if (lineNum > 1 && cols[0]) {
						// not interested in header or empty line
						stmt.addBatch(cols)
					}
				}
			}
		}
		
		config.logger.log("Processed ${lineNum} lines")
	}
	
	private void processRBMFile(File f, Sql sql, Object studyInfo) {
		def lineNum = 0
		def header_mappings = [:]
		def header_antigen = [:]
		
		config.logger.log("Processing RBM data: ${f.name}")
		
		sql.withTransaction {
			sql.withBatch(100, """\
					INSERT INTO stg_subject_rbm_data
					(trial_name, antigen_name, value_text, timepoint, assay_id, sample_id, subject_id, site_id)
					VALUES
					(:study_id, :antigen_name, :value_text, :visit_name, :assay_id, :sample_id, :subject_id, :site_id)
					""") {
				stmt ->
			
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
					
					if (lineNum == 1) {
						// header
						def resetDataStart = true
						
						cols.eachWithIndex {
							val, i ->
							
							if (val ==~ /(?i)study_id/) { header_mappings['study_id'] = i; resetDataStart = true; }
							else if (val ==~ /(?i)subject_id/) { header_mappings['subject_id'] = i; resetDataStart = true; }
							else if (val ==~ /(?i)sample_id/) { header_mappings['sample_id'] = i; resetDataStart = true; }
							else if (val ==~ /(?i)visit_name/) { header_mappings['visit_name'] = i; resetDataStart = true; }
							else if (val ==~ /(?i)site_id/) { header_mappings['site_id'] = i; resetDataStart = true; }
							else if (val ==~ /(?i)assay_id/) { header_mappings['assay_id'] = i; resetDataStart = true; }
							else {
								if (! header_mappings['_DATA_START_COL'] && resetDataStart) {
									header_mappings['_DATA_START_COL'] = i
									resetDataStart = false
								}
								
								header_antigen[i] = val // we could do it simpler, but then we wouldn't be able to track unknown columns
							}
						}
						
						if (! (
							header_mappings.containsKey('study_id')
							&& header_mappings.containsKey('subject_id')
						) ) {
							throw new Exception("Study ID and Subject ID columns are not defined")					
						}
						
						if (! header_mappings['_DATA_START_COL'])
							throw new Exception("Can't determine start of data columns")
					
					}
					else {
						// data line
						def out = [:]
						['study_id', 'subject_id', 'sample_id', 'visit_name', 'site_id', 'assay_id'].each {
							out[it] = getColumnValue(cols, header_mappings, it)
						}
						
						(header_mappings['_DATA_START_COL']..cols.size()-1).each {
							out['antigen_name'] = header_antigen[it]
							out['value_text'] = cols[it]
							stmt.addBatch(out)
						}
					}
				}
			}
		}
		
		// OK, now we need to retrieve studyID & node
		def rows = sql.rows("select trial_name, count(*) as cnt from stg_subject_rbm_data group by trial_name")
		def rsize = rows.size()
		
		if (rsize > 0) {
			if (rsize == 1) {
				def studyId = rows[0].trial_name
				if (studyId) {
					studyInfo['id'] = studyId
				}
				else {
					throw new Exception("Study ID is null!")
				}
			}
			else {
				throw new Exception("Multiple StudyIDs are detected!")
			}
		}
		else {
			throw new Exception("Study ID is not specified!")
		}
		
		config.logger.log("Processed ${lineNum} lines")
	}

	@Override
	public boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
		def studyId = studyInfo['id']
		def studyNode = studyInfo['node']
		if (studyId && studyNode) {
			config.logger.log("Study ID=${studyId}; Node=${studyNode}")
			sql.call("{call i2b2_process_rbm_data($studyId,$jobId)}")
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
			return false;
		}
		
		return true;
	}

	@Override
	public String getProcedureName() {
		return "I2B2_PROCESS_RBM_DATA";
	}
	
	private String getColumnValue(cols, header_mappings, String label) {
		if (header_mappings.containsKey(label))
			return fixColumn(cols[header_mappings[label]])
		else
			return null
	}
	
	private String fixColumn(String s) {
		if ( s == null ) return '';
		
		def res = s.trim()
		res = (res =~ /(?s)^\"(.+)\"$/).replaceFirst('$1')
	
		return res
	}

}
