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
  

package com.recomdata.pipeline.loader

import java.util.Properties;

import com.recomdata.pipeline.util.Util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

import com.recomdata.pipeline.transmart.BioAssayPlatform
import com.recomdata.pipeline.transmart.BioObservation

class AssayPlatform {

	private static final Logger log = Logger.getLogger(AssayPlatform)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");
		
		log.info("Start loading property file AssayPlatform.properties ...")
		Properties props = Util.loadConfiguration("conf/AssayPlatform.properties");

		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

		AssayPlatform ap = new AssayPlatform()
		
		ArrayList platform = new ArrayList()
		platform = ap.readBioAssayPlatform(props)
		
		BioAssayPlatform bap = ap.getBioAssayPlatform(biomart)
		ap.loadBioAssayPlatform(biomart,  platform, bap)
	}

	
	void loadBioAssayPlatform(Sql sql, ArrayList platform, BioAssayPlatform bap){
		
		for(int i=0; i<platform.size(); i++){
			println platform[i]
			bap.loadBioAssayPlatform2(platform[i])
		}
	}


	ArrayList readBioAssayPlatform(Properties props){

		ArrayList alPlatform = new ArrayList()

		Map columnMap = [:]
		Map platform = [:]

		File f = new File(props.get("platform_source_file"))
		if(f.size() > 0){
			f.eachLine{
				String [] str = it.split(",")
				if(it.toUpperCase().indexOf("BIO_ASSAY_PLATFORM") != -1){
					println it
					for(int i=0; i<str.size(); i++){
						columnMap[i] = str[i].trim().toLowerCase()
					}
				} else{
					println it
					for(int i=0; i<str.size(); i++){
						platform[columnMap[i]] = str[i].trim()
					}
					alPlatform.add(platform)
					platform = [:]
				}
			}
		} else{
			log.info "the file: ${f.toString()} is empty or not exist ..."
		}

		return alPlatform
	}
	
	
	BioAssayPlatform getBioAssayPlatform(Sql biomart){
		BioAssayPlatform bap = new BioAssayPlatform()
		bap.setBiomart(biomart)
		return bap
	}
}
