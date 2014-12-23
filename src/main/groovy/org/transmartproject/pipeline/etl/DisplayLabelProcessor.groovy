/*************************************************************************
 * 
 * tranSMART Data Loader - ETL tool for tranSMART
 * 
 * Copyright 2012-2013 Thomson Reuters
 * 
 * This product includes software developed at Thomson Reuters
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License  as published by
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
