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

import org.apache.log4j.Logger;

import groovy.sql.Sql;

import com.recomdata.pipeline.util.Util


class BioContentReference {

	private static final Logger log = Logger.getLogger(BioContentReference)

	Sql biomart

	void loadBioContentReferenceForKEGG(){
		String qry = """ insert into bio_content_reference(bio_content_id, bio_data_id, content_reference_type)
						 select distinct bc.bio_file_content_id, path.bio_marker_id, bcr.location_type
						 from bio_content bc, bio_marker path, bio_content_repository bcr
						 where bc.repository_id = bcr.bio_content_repo_id
							  and path.primary_external_id=substr(bc.location, length(bc.location)-7)
							  and path.primary_source_code='KEGG' and
							  (bc.bio_file_content_id, path.bio_marker_id) not in 
							    (select bio_content_id, bio_data_id from bio_content_reference)
		  			"""
		biomart.execute(qry)
	}


	void insertBioContentReference(long contentId, long dataId, String referenceType, String studyName){

		String qry = """ insert into bio_content_reference(bio_content_id, bio_data_id, content_reference_type, etl_id_c) values(?, ?, ?, ?) """

		if(isBioContentReferenceExist(contentId, dataId)){
			log.info "$contentId:$dataId already exists in BIO_CONTENT_REFERENCE ..."
		}else{
			log.info "Insert $contentId:$dataId into BIO_CONTENT_REFERENCE ..."
			biomart.execute(qry, [
				contentId,
				dataId,
				referenceType,
				studyName
			])
		}
	}


	boolean isBioContentReferenceExist(long contentId, long dataId){
		String qry = """ select count(*) from bio_content_reference 
		                 where bio_content_id=? and bio_data_id=? """
		def res = biomart.firstRow(qry, [contentId, dataId])
		if(res[0] > 0) return true
		else return false
	}


	long getBioDataContentReferenceId(long contentId, long dataId){
		String qry = """ select bio_content_reference_id 
		                 from bio_content_reference
						 where correlation=? and type_name=?"""
		def res = biomart.firstRow(qry, [contentId, dataId])
		if(res.equals(null)) return 0
		else return res[0]
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
