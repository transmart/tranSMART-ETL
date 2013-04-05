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

import java.util.Map;

import org.apache.log4j.Logger;

import groovy.sql.Sql;

import com.recomdata.pipeline.util.Util


class GplInfo {

	private static final Logger log = Logger.getLogger(GplInfo)

	Sql deapp

	void insertGplInfo(String platform, String title, String organism, String markerType){

		String qry = "insert into de_gpl_info(platform,title,organism,annotation_date,marker_type) values(?,?,?,sysdate,?)"

		if(isGplInfoExist(platform, markerType)){
			log.info "$platform:$markerType already exists in DE_GPL_INFO ..."
		}else{
			log.info "Insert $platform:$markerType into DE_GPL_INFO ..."
			deapp.execute(qry, [
				platform,
				title,
				organism,
				markerType
			])
		}
	}


	void insertGplInfo(Map gplInfoMap){

		log.info "Start inserting data into DE_GPL_INFO ... "

		if(isGplInfoExist(gplInfoMap["platform"], gplInfoMap["markerType"])){
			log.info "Platform [${gplInfoMap["platform"]}] already extists in DE_GPL_INFO ... "
		}else{
			String qry = """insert into de_gpl_info (platform, title, organism, annotation_date, marker_type, release_nbr) values(?, ?, ?, ?, ?,?) """
			deapp.execute(qry, [
				gplInfoMap["platform"],
				gplInfoMap["title"],
				gplInfoMap["organism"],
				gplInfoMap["annotationDate"],
				gplInfoMap["markerType"],
				gplInfoMap["releaseNbr"]
			])
			log.info "Platform ${gplInfoMap["platform"]} is inserted into DE_GPL_INFO ... "
		}
		log.info "End loading data into the table DE_SNP_INFO ... "
	}


	boolean isGplInfoExist(String platform, String markerType){
		String qry = "select count(*) from de_gpl_info where platform=? and marker_type=?"
		def res = deapp.firstRow(qry, [platform, markerType])
		if(res[0] > 0) return true
		else return false
	}


	void setDeapp(Sql deapp){
		this.deapp = deapp
	}
}
