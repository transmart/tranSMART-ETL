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
  

package com.recomdata.pipeline.converter

import groovy.sql.Sql
import org.apache.log4j.Logger;

class PlinkMapGenerator {

	private static final Logger log = Logger.getLogger(Converter)

	Sql sql
	File mapFile

	void createPlinkMapFromSampleMapping(Map sampleMapping){
		sampleMapping.each{key, val ->
			println key + ":" + val
		}
	}


	long getPatientNumberByGSMNumber(String gsmNumber){
	}


	void createMapforGPL13314(){
		StringBuffer mapData = new StringBuffer()
		if(mapFile.exists()){
			mapFile.delete()
			mapFile.createNewFile()
		} else {
			log.info("Create the file: " + mapFile.toString())
			mapFile.createNewFile()
		}

		String qry = "select decode(chr,'X','23','Y','24','XY','25','MT','26', chr) chr, id, mapinfo from GPL13314"
		int index = 1
		sql.eachRow(qry) {
			mapData.append(it.chr + "\t")
			mapData.append(it.id + "\t")
			mapData.append("0\t")
			mapData.append(it.mapinfo + "\t\n")
			index++
		}
		log.info "Write to MAP file: " + mapFile.toString() + " and total SNPs: " + index
		mapFile.append(mapData.toString())
	}


	void setMapFile(File mapFile){
		this.mapFile = mapFile
	}

	void setSql(Sql sql){
		this.sql = sql
	}
}
