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

class BioAssayDataset {

	private static final Logger log = Logger.getLogger(BioAssayDataset)

	Sql biomart
	String testsDataTable

	void loadBioAssayDataset(){

		String qry = """ select distinct name, test, platform, bio_experiment_id 
						 from $testsDataTable t1, bio_experiment t2
						 where t1.name=t2.accession """
		biomart.eachRow(qry) {
			String [] analysis = it.test.split (/=>/)[1].trim().split(/ vs /)

			String analysis1 = analysis[0].trim()
			String analysis2 = analysis[1].trim()
			String accession = it.name
			String platform = it.platform
			long bioExperimentId = it.bio_experiment_id
			loadBioAssayDataset(analysis1, accession, platform, bioExperimentId)
			loadBioAssayDataset(analysis2, accession, platform, bioExperimentId)
		}
	}


	void loadBioAssayDataset(String datasetName, String accession, String platform, long bioExperimentId){

		if(isBioAssayDatasetExist(datasetName, bioExperimentId)){
			log.info "$datasetName ($accession:$platform) already exists in BIO_ASSAY_DATASET ..."
		}else{
			log.info "Start inserting $datasetName ($accession:$platform) into BIO_ASSAY_DATASET ..."

			String qry = """ insert into bio_assay_dataset(bio_experiment_id, dataset_name, dataset_description, 
										accession, create_date) 
							 values(?, ?, ?, ?, sysdate) """
			biomart.execute(qry, [
				bioExperimentId,
				datasetName,
				accession + " " + platform,
				accession
			])

			log.info "End loading $datasetName into BIO_ASSAY_DATASET ..."
		}
	}


	boolean isBioAssayDatasetExist(String datasetName, long bioExperimentId){
		String qry = "select count(1) from bio_assay_dataset where dataset_name=? and bio_experiment_id=?"
		if(biomart.firstRow(qry, [
			datasetName,
			bioExperimentId
		])[0] > 0){
			return true
		}else{
			return false
		}
	}


	void setTestsDataTable(String testsDataTable){
		this.testsDataTable = testsDataTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
