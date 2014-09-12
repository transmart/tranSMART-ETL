/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 * 

 ******************************************************************/

package org.transmartproject.pipeline.etl

import groovy.sql.Sql

import java.io.File;

class DisplayLabelProcessor extends DataProcessor {

	public DisplayLabelProcessor(Object conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean processFiles(File f, Sql sql, Object studyInfo) {
		config.logger.log("DisplayLabel processFiles")
		config.logger.log("truncating table tm_lz.lt_src_display_mapping")

		sql.execute('TRUNCATE TABLE tm_lz.LT_SRC_DISPLAY_MAPPING')
		
		def lineNum = 0
		
		def header_mappings = [:]
		
		sql.withTransaction {
			sql.withBatch(100, """\
					INSERT into tm_lz.LT_SRC_DISPLAY_MAPPING
					(
						CATEGORY_CD, 
						DISPLAY_VALUE, 
						DISPLAY_LABEL, 
						DISPLAY_UNIT
						
					)
					VALUES 
					(:category_cd, :display_value, :display_unit,  :display_label
					
					)
					""") {
				stmt ->
				
					
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
					def output = [
						category_cd : cols[0],
						display_value : cols[1],
						display_unit : cols[2],
						display_label : cols[3],
					]
					println lineNum
					println "category_cd-"+cols[0]
					println "display_value-"+cols[1]
					println "display_unit-"+cols[2]
					println "display_label-"+cols[3]
					
					stmt.addBatch(output)
					
				}
				
			}
		}
		config.logger.log("DisplayLabel processed ${lineNum} lines in file ${f}")
		sql.commit()
		config.logger.log("DisplayLabel processFiles returning true")
		return true;
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

	@Override
	public boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
		//nothing to do return true
		return true;
	}

	@Override
	public String getProcedureName() {
		return 'No Procedure required - display mapping will process during clinical load';
	}

}
