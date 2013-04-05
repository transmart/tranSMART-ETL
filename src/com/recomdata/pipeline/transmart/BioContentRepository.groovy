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


class BioContentRepository {

	private static final Logger log = Logger.getLogger(BioContentRepository)

	Sql biomart

	void insertBioContentRepository(String location, String isActive, String repositoryType, String locationType){

		String qry = """ insert into bio_content_repository(location, active_y_n, repository_type, location_type) values(?, ?, ?, ?) """

		if(isBioContentRepositoryExist(location, repositoryType)){
			log.info "\"$location:$repositoryType\" already exists in BIO_CONTENT_REPOSITORY ..."
		}else{
			log.info "Insert \"$location:$repositoryType\" into BIO_CONTENT_REPOSITORY ..."
			biomart.execute(qry, [
				location,
				isActive,
				repositoryType,
				locationType
			])
		}
	}


	boolean isBioContentRepositoryExist(String location, String type){
		String qry = "select count(*) from bio_content_repository where location=? and repository_type=?"
		def res = biomart.firstRow(qry, [location, type])
		if(res[0] > 0) return true
		else return false
	}

	
	long getBioContentRepositoryId(String location, String type){
		String qry = """ select bio_content_repo_id from bio_content_repository
						 where location=? and repository_type=?"""
		def res = biomart.firstRow(qry, [location, type])
		if(res.equals(null)) return 0
		else return res[0]
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
