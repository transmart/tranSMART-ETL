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
  

package com.recomdata.pipeline.i2b2

import org.apache.log4j.Logger;

import groovy.sql.Sql

class ConceptDimension {

	private static final Logger log = Logger.getLogger(ConceptDimension)

	Sql i2b2demodata
	String tableName, studyName

	void loadConceptDimensions(List concepts){
		concepts.each{ insertConceptDimension(it) }
	}


	Map getConceptCode(List concepts){

		Map conceptPathToCode = [:]

		concepts.each{
			String conceptCode = getConceptCode(it)
			if(!conceptCode.equals(null)){
				conceptPathToCode[it] = conceptCode
			}
		}
		return conceptPathToCode
	}


	String getConceptCode(String conceptPath){

		String qry = "select concept_cd from concept_dimension where concept_path=?"

		def res = i2b2demodata.firstRow(qry, [conceptPath.replace("/", "\\")])
		if(res.equals(null)){
			log.info "No concept code for the concept: $conceptPath"
			return null
		}else{
			return res[0]
		}
	}

	/**
	 * Create the following trigger for CONCEPT_DIMENSION:
	 * 
	 create or replace TRIGGER "TRG_CONCEPT_DIMENSION_CD"
	 before insert on "CONCEPT_DIMENSION"
	 for each row begin
	 if inserting then
	 if :NEW."CONCEPT_CD" is null then
	 select TM_CZ.CONCEPT_ID.nextval into :NEW."CONCEPT_CD" from dual;
	 end if;
	 end if;
	 end;
	 * 	
	 * @param conceptPath
	 * @param nameChar
	 * @param trialName
	 */
	void insertConceptDimension(String conceptPath){

		if(tableName.equals(null)) tableName = "CONCEPT_DIMENSION"

		String [] str = conceptPath.split("/")
		String nameChar = str[str.size()-1]
		log.info str.size() + ":\t" + nameChar

		String qry = "insert into concept_dimension(concept_path, name_char, sourcesystem_cd, table_name) values(?,?,?,?)"

		String concept = conceptPath.replace("/", "\\")
		if(isConceptDimensonExist(concept)){
			log.info "$conceptPath already exists ..."
		}else{
			i2b2demodata.execute(qry, [
				concept,
				nameChar,
				studyName,
				tableName
			])
		}
	}


	boolean isConceptDimensonExist(String conceptPath){
		String qry = "select count(*) from concept_dimension where concept_path=?"
		def res = i2b2demodata.firstRow(qry, [conceptPath])
		if(res[0] > 0) return true
		else return false
	}


	void setI2b2demodata(Sql i2b2demodata){
		this.i2b2demodata = i2b2demodata
	}


	void setTableName(String tableName){
		this.tableName = tableName
	}


	void setStudyName(String studyName){
		this.studyName = studyName
	}
}
