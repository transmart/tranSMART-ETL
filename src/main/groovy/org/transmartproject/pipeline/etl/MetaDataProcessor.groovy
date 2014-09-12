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

class MetaDataProcessor extends DataProcessor {

	public MetaDataProcessor(Object conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean processFiles(File f, Sql sql, Object studyInfo) {
		
		sql.execute('TRUNCATE TABLE tm_lz.lt_src_study_metadata')
		
		def lineNum = 0
		
		def header_mappings = [:]
		
		sql.withTransaction {
			sql.withBatch(100, """\
					INSERT into tm_lz.lt_src_study_metadata
					(
						STUDY_ID, 
						TITLE, 
						DESCRIPTION, 
						DESIGN, 
						START_DATE, 
						COMPLETION_DATE, 
						PRIMARY_INVESTIGATOR, 
						CONTACT_FIELD, 
						STATUS, 
						OVERALL_DESIGN, 
						INSTITUTION, 
						COUNTRY, 
						BIOMARKER_TYPE, 
						TARGET, 
						ACCESS_TYPE, 
						STUDY_OWNER, 
						STUDY_PHASE, 
						BLINDING_PROCEDURE, 
						STUDYTYPE, 
						DURATION_OF_STUDY_WEEKS, 
						NUMBER_OF_PATIENTS, 
						NUMBER_OF_SITES, 
						ROUTE_OF_ADMINISTRATION, 
						DOSING_REGIMEN, 
						GROUP_ASSIGNMENT, 
						TYPE_OF_CONTROL, 
						PRIMARY_END_POINTS, 
						SECONDARY_END_POINTS, 
						INCLUSION_CRITERIA, 
						EXCLUSION_CRITERIA, 
						SUBJECTS, 
						GENDER_RESTRICTION_MFB, 
						MIN_AGE, 
						MAX_AGE, 
						SECONDARY_IDS, 
						DEVELOPMENT_PARTNER, 
						GEO_PLATFORM, 
						MAIN_FINDINGS, 
						SEARCH_AREA, 
						COMPOUND, 
						DISEASE, 
						PUBMED_IDS, 
						ORGANISM
					)
					VALUES 
					(
						?, 
						?, 
						?, 
						?, 
						NULL,
						?, 
						?, 
						NULL,
						NULL,
						NULL,
						NULL,
						NULL,
						NULL,
						NULL,
						NULL,
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?, 
						?,
						?, 
						?, 
						NULL,
						?
					)
					""") {
				stmt ->
				
				def prevCols = [] // to support multiline values
		
				f.splitEachLine("\t") {
					cols ->
					
					lineNum++
				
					if (lineNum == 1) {
						// parsing header line
						cols.eachWithIndex {
							val, i ->
							
							val = fixColumn(val)
							
							if (val ==~ /(?i)Accession \(Internal study_id\)/) header_mappings['study_id'] = i
							else if (val ==~ /(?i)Title/) header_mappings['title'] = i
							else if (val ==~ /(?i)Description/) header_mappings['description'] = i
							else if (val ==~ /(?i)Study design/) header_mappings['study_design'] = i
							else if (val ==~ /(?i)Completion date/) header_mappings['completion_date'] = i
							else if (val ==~ /(?i)PI/) header_mappings['pi'] = i
							else if (val ==~ /(?i)Study Owner/) header_mappings['study_owner'] = i
							else if (val ==~ /(?i)Study Phase/) header_mappings['study_phase'] = i
							else if (val ==~ /(?i)Blinding Procedure/) header_mappings['blinding_procedure'] = i
							else if (val ==~ /(?i)Study Type/) header_mappings['study_type'] = i
							else if (val ==~ /(?i)Duration of Study/) header_mappings['duration_of_study'] = i
							else if (val ==~ /(?i)Number of Patients/) header_mappings['number_of_patients'] = i
							else if (val ==~ /(?i)Number of Sites/) header_mappings['number_of_sites'] = i
							else if (val ==~ /(?i)Route of Administration/) header_mappings['route_of_administration'] = i
							else if (val ==~ /(?i)Dosing Regimen/) header_mappings['dosing_regimen'] = i
							else if (val ==~ /(?i)Group Assignment/) header_mappings['group_assignment'] = i
							else if (val ==~ /(?i)Type of Control/) header_mappings['type_of_control'] = i
							else if (val ==~ /(?i)Primary Endpoints/) header_mappings['primary_endpoints'] = i
							else if (val ==~ /(?i)Secondary Endpoints/) header_mappings['secondary_endpoints'] = i
							else if (val ==~ /(?i)Inclusion Criteria/) header_mappings['inclusion_criteria'] = i
							else if (val ==~ /(?i)Exclusion Criteria/) header_mappings['exclusion_criteria'] = i
							else if (val ==~ /(?i)Subjects/) header_mappings['subjects'] = i
							else if (val ==~ /(?i)Gender Restriction/) header_mappings['gender_restriction'] = i
							else if (val ==~ /(?i)Min.* Age/) header_mappings['min_age'] = i
							else if (val ==~ /(?i)Max.* Age/) header_mappings['max_age'] = i
							else if (val ==~ /(?i)Secondary IDs/) header_mappings['secondary_ids'] = i
							else if (val ==~ /(?i)Development Partner/) header_mappings['development_partner'] = i
							else if (val ==~ /(?i)GEO Platform/) header_mappings['geo_platform'] = i
							else if (val ==~ /(?i)Main Findings/) header_mappings['main_findings'] = i
							else if (val ==~ /(?i)Area/) header_mappings['area'] = i
							else if (val ==~ /(?i)Drug name/) header_mappings['drug_name'] = i
							else if (val ==~ /(?i)Condition/) header_mappings['condition'] = i
							else if (val ==~ /(?i)(Species|Organism)/) header_mappings['species'] = i
						}
						
						if (! header_mappings.containsKey('study_id')) {
							throw new Exception("Study ID column is not defined")
						}
						
					}	
					else {
						// line with data
						
						// checking if it's continuation of multiline string
						if (prevCols) {
							def firstCol = cols[0]
							prevCols[prevCols.size()-1] += "\n${firstCol}" // join last column with the first in the new batch
							if (cols.size() > 1)
								prevCols.addAll(cols[1..cols.size()-1]) // join other columns
							cols = prevCols.toList().clone();
							
							// check if it's an ending of multiline string
							if (firstCol ==~ /[^"]*"/) 
								prevCols = []
						}
						
						// check if it's a beginning of multiline string
						if (cols.last() ==~ /"[^"]+/ || cols.size() < 2) {
							prevCols = cols.toList().clone();
							return // next loop
						}
						
						if (cols[0]) // line not empty
						{
						
							if (! ( getColumnValue(cols, header_mappings, 'study_id')  
									&& getColumnValue(cols, header_mappings, 'title') ) ) {
								throw new Exception("Study ID or Title are not defined at line ${lineNum}")
							}
							
							def species = getColumnValue(cols, header_mappings, 'species')?:'Homo Sapiens'		
									
							stmt.addBatch([
								getColumnValue(cols, header_mappings, 'study_id'), 
								getColumnValue(cols, header_mappings, 'title'), 
								getColumnValue(cols, header_mappings, 'description'),
								getColumnValue(cols, header_mappings, 'study_design'), 
								getColumnValue(cols, header_mappings, 'completion_date'), 
								getColumnValue(cols, header_mappings, 'pi'),
								getColumnValue(cols, header_mappings, 'study_owner'), 
								getColumnValue(cols, header_mappings, 'study_phase'), 
								getColumnValue(cols, header_mappings, 'blinding_procedure'),
								getColumnValue(cols, header_mappings, 'study_type'), 
								getColumnValue(cols, header_mappings, 'duration_of_study'), 
								getColumnValue(cols, header_mappings, 'number_of_patients'),
								getColumnValue(cols, header_mappings, 'number_of_sites'), 
								getColumnValue(cols, header_mappings, 'route_of_administration'),
								getColumnValue(cols, header_mappings, 'dosing_regimen'), 
								getColumnValue(cols, header_mappings, 'group_assignment'), 
								getColumnValue(cols, header_mappings, 'type_of_control'),
								getColumnValue(cols, header_mappings, 'primary_endpoints'), 
								getColumnValue(cols, header_mappings, 'secondary_endpoints'),
								getColumnValue(cols, header_mappings, 'inclusion_criteria'), 
								getColumnValue(cols, header_mappings, 'exclusion_criteria'),
								getColumnValue(cols, header_mappings, 'subjects'), 
								getColumnValue(cols, header_mappings, 'gender_restriction'), 
								getColumnValue(cols, header_mappings, 'min_age'),
								getColumnValue(cols, header_mappings, 'max_age'), 
								getColumnValue(cols, header_mappings, 'secondary_ids'), 
								getColumnValue(cols, header_mappings, 'development_partner'),
								getColumnValue(cols, header_mappings, 'geo_platform'), 
								getColumnValue(cols, header_mappings, 'main_findings'), 
								getColumnValue(cols, header_mappings, 'area'),
								getColumnValue(cols, header_mappings, 'drug_name'), 
								getColumnValue(cols, header_mappings, 'condition'), 
								species
							])
						}
					}
				}
			}
		}
		
		config.logger.log("Processed ${lineNum} lines")
		
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
		sql.call("{call i2b2_load_study_metadata($jobId)}")
		return true;
	}

	@Override
	public String getProcedureName() {
		return 'I2B2_LOAD_STUDY_METADATA';
	}

}
