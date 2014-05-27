/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/
  

package com.recomdata.pipeline.transmart

import groovy.sql.Sql;

import org.apache.log4j.Logger;

class BioDataOmicMarker {

	private static final Logger log = Logger.getLogger(BioDataOmicMarker)

	Sql biomart
	String testsDataTable, gxAnnotationTable

	void loadBioDataOmicMarker(String gseAnalysis){
		
		log.info "Start loading data into Bio_Data_Omic_Marker ..."
		
		String qry = """ insert into bio_data_omic_marker nologging
								(bio_data_id, bio_marker_id, data_table)
						 select a.bio_asy_analysis_data_id, c.bio_marker_id, 'BAAD' 
						 from bio_assay_analysis_data a, bio_assay_data_annotation c, $gseAnalysis t 
						 where a.BIO_ASSAY_FEATURE_GROUP_ID = c.BIO_ASSAY_FEATURE_GROUP_ID 
						   		and a.bio_experiment_id=t.bio_experiment_id
						 minus
						 select bio_data_id, bio_marker_id, data_table from bio_data_omic_marker """

		biomart.execute(qry)
		
		log.info "End loading data into Bio_Data_Omic_Marker ..."
	}

	
	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
