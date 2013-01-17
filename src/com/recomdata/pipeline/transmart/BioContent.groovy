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


class BioContent {

	private static final Logger log = Logger.getLogger(BioContent)

	Sql biomart

	void loadBioContentForKEGG(){
		String qry = """ insert into bio_content (repository_id, location, file_type)
	                     select distinct bcr.bio_content_repo_id
								, bcr.location||'dbget-bin/show_pathway?'|| bm.primary_external_id as location
								, 'Data'
						from bio_content_repository bcr, bio_marker bm
						where upper(bcr.repository_type)='KEGG'
							and upper(bm.primary_source_code)='KEGG' 
							and (bcr.bio_content_repo_id, location) not in 
							  (select repository_id, location from bio_content)
					"""
		biomart.execute(qry)
	}


	void insertBioContent(long repository_id, String location, String fileType, String studyName){

		String qry = """ insert into bio_content(repository_id, location, file_type, etl_id_c) values(?, ?, ?, ?) """

		if(isBioContentExist(repository_id, location)){
			log.info "$repository_id:$location already exists in BIO_CONTENT ..."
		}else{
			log.info "Insert $repository_id:$location into BIO_CONTENT ..."
			biomart.execute(qry, [
				repository_id,
				location,
				fileType,
				studyName
			])
		}
	}


	boolean isBioContentExist(long repository_id, String location){
		String qry = "select count(*) from bio_content where repository_id=? and location=?"
		def res = biomart.firstRow(qry, [repository_id, location])
		if(res[0] > 0) return true
		else return false
	}


	long getBioContentId(long repository_id, String location){
		String qry = """ select bio_file_content_id from bio_content
						 where repository_id=? and location=?"""
		def res = biomart.firstRow(qry, [repository_id, location])
		if(res.equals(null)) return 0
		else return res[0]
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
