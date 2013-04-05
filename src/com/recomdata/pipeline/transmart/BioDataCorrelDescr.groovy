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


class BioDataCorrelDescr {

	private static final Logger log = Logger.getLogger(BioDataCorrelDescr)

	Sql biomart

	void insertBioDataCorrelDescr(String correlation, String description, String type){

		String qry = """ insert into bio_data_correl_descr(correlation, description, type_name) values(?, ?, ?) """

		if(isBioDataCorrelDescrExist(correlation, type)){
			log.info "$correlation:$type already exists in BIO_DATA_CORREL_DESCR ..."
		}else{
			log.info "Insert $correlation:$type into BIO_DATA_CORREL_DESCR ..."
			biomart.execute(qry, [
				correlation,
				description,
				type
			])
		}
	}


	boolean isBioDataCorrelDescrExist(String correlation, String type){
		String qry = "select count(*) from bio_data_correl_descr where correlation=? and type_name=?"
		def res = biomart.firstRow(qry, [correlation, type])
		if(res[0] > 0) return true
		else return false
	}

	
	long getBioDataCorrelId(String correlation, String type){
		String qry = """ select bio_data_correl_descr_id from bio_data_correl_descr 
					     where correlation=? and type_name=?"""
		def res = biomart.firstRow(qry, [correlation, type])
		if(res.equals(null)) return 0
		else return res[0] 
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
