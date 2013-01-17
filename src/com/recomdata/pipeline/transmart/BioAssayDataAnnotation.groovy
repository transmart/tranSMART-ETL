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

class BioAssayDataAnnotation {

	private static final Logger log = Logger.getLogger(BioAssayDataAnnotation)

	Sql biomart
	String testsDataTable, gxAnnotationTable

	void loadBioAssayDataAnnotation(){
		log.info "Start loading new probes into BIO_ASSAY_DATA_ANNOTATION ..."
		
		String qry = """ insert into bio_assay_data_annotation(bio_assay_feature_group_id, bio_marker_id)
						 select t1.bio_assay_feature_group_id, t3.bio_marker_id
						 from bio_assay_feature_group t1, $gxAnnotationTable t2, bio_marker t3, gse_analysis t4
						 where upper(t1.feature_group_name)=upper(t2.probe_id) 
						 	and to_char(t2.gene_symbol) = to_char(t3.bio_marker_name) 
						 	and upper(t2.species)=to_char(upper(t3.organism))
							and upper(t2.platform)=upper(t4.platform)
							and t3.bio_marker_type='GENE'
						minus
						select bio_assay_feature_group_id, bio_marker_id
						from bio_assay_data_annotation """

		biomart.execute(qry)
		log.info "End loading new probes into BIO_ASSAY_DATA_ANNOTATION ..."
	}


	void setGxAnnotationTable(String gxAnnotationTable){
		this.gxAnnotationTable = gxAnnotationTable
	}
	
	
	void setTestsDataTable(String testsDataTable){
		this.testsDataTable = testsDataTable
	}

	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
