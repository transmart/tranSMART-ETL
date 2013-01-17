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

class BioAssayAnalysisPlatform {

	private static final Logger log = Logger.getLogger(BioAssayAnalysisPlatform)

	Sql biomart

	void loadBioAssayAnalysisPlatform(String analysisPlatformName){

		if(isBioAssayAnalysisPlatformExist(analysisPlatformName)){
			log.info "$analysisPlatformName already exists in BIO_ASY_ANALYSIS_PLTFM ..."
		}else{
			log.info "Start loading $analysisPlatformName into BIO_ASY_ANALYSIS_PLTFM ..."

			String qry = """ insert into bio_asy_analysis_pltfm(platform_name) values(?) """
			biomart.execute(qry, [analysisPlatformName])

			log.info "End loading $analysisPlatformName into BIO_ASY_ANALYSIS_PLTFM ..."
		}
	}


	boolean isBioAssayAnalysisPlatformExist(String analysisPlatformName){
		String qry = "select count(1) from bio_asy_analysis_pltfm where upper(platform_name)=?"
		if(biomart.firstRow(qry, [
			analysisPlatformName.toUpperCase()
		])[0] > 0){
			return true
		}else{
			return false
		}
	}

	long getBioAssayAnalysisPlatformId(String analysisPlatformName){
		String qry = "select bio_asy_analysis_pltfm_id from bio_asy_analysis_pltfm where upper(platform_name)=?"
		def rs = biomart.firstRow(qry, [
			analysisPlatformName.toUpperCase()
		])
		if(rs.equals(null)){
			return 0
		}else{
			return rs[0]
		}
	}

	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
