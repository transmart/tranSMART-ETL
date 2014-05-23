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

class I2b2 {

	private static final Logger log = Logger.getLogger(I2b2)

	Sql i2b2metadata
	String studyName
	Map visualAttrs

	
	void loadConceptPaths(Map conceptPathToCode){

		conceptPathToCode.each{key, val ->
			loadConceptPath(key, val)
		}
	}

	
	void loadConceptPath(String conceptPath, String conceptCode){

		String qry = """ INSERT INTO I2B2 (c_hlevel, C_FULLNAME, C_NAME, C_VISUALATTRIBUTES, c_synonym_cd,
								C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_DIMCODE, C_TOOLTIP,
								SOURCESYSTEM_CD, c_basecode, C_OPERATOR, c_columndatatype, c_comment, i2b2_id)
						 VALUES(?, ?, ?, ?, 'N',  
							   'CONCEPT_CD', 'CONCEPT_DIMENSION', 'CONCEPT_PATH', ?, ?,
								?, ?, 'LIKE', 'T',
								?, i2b2_id_seq.nextval)""";

		String [] str = conceptPath.split("/")
		int c_hlevel = str.size() - 2
		String c_name = str[str.size() - 1]
		String path = conceptPath.replace("/", "\\")
		String c_comment = "trial:" + studyName
		String visualAttr = visualAttrs[conceptPath]
		
		if(isI2b2Exist(path)){
			log.info "$conceptPath already exists ..."
		}else{
			log.info "insert concept path: $conceptPath into I2B2 ..."
			i2b2metadata.execute(qry, [c_hlevel, path, c_name, visualAttr, path, path, studyName, conceptCode, c_comment])
		}
	}

	/**
	 * insert I2B2 through selecting from concept_dimension and is copied from Jean's Store Procedure: I2B2_ADD_NODE 
	 * 
	 create or replace TRIGGER "TRG_I2B2_C_BASECODE"
	 before insert on "I2B2"
	 for each row begin
	 if inserting then
	 if :NEW."C_BASECODE" is null then
	 select TM_CZ.CONCEPT_ID.nextval into :NEW."C_BASECODE" from dual;
	 end if;
	 end if;
	 end;
	 */

	void insertI2B2(String conceptPath){

		String qry = """ INSERT INTO I2B2 (c_hlevel, C_FULLNAME, C_NAME, C_VISUALATTRIBUTES, c_synonym_cd, 
								C_FACTTABLECOLUMN, C_TABLENAME, C_COLUMNNAME, C_DIMCODE, C_TOOLTIP, 
								SOURCESYSTEM_CD, c_basecode, C_OPERATOR, c_columndatatype, c_comment, i2b2_id)
	                     SELECT (length(concept_path) - nvl(length(replace(concept_path, '\')),0))/length('\') - 2 + root_level,
								CONCEPT_PATH, NAME_CHAR, 'FA', 'N',
		 						'CONCEPT_CD', 'CONCEPT_DIMENSION', 'CONCEPT_PATH', CONCEPT_PATH, CONCEPT_PATH,
		 						SOURCESYSTEM_CD, CONCEPT_CD, 'LIKE', 'T', 
		 						decode(TrialId,null,null,'trial:' || TrialID), i2b2_id_seq.nextval
					     FROM CONCEPT_DIMENSION
						 WHERE CONCEPT_PATH = ?""";

		if(isI2b2Exist(conceptPath)){
			log.info "$conceptPath already exists ..."
		}else{
			log.info "insert concept path: $conceptPath into I2B2 ..."
			i2b2metadata.execute(qry, [conceptPath])
		}
	}



	boolean isI2b2Exist(String conceptPath){
		String qry = "select count(*) from i2b2 where c_fullname=?"
		def res = i2b2metadata.firstRow(qry, [conceptPath])
		if(res[0] > 0) return true
		else return false
	}


	void setI2b2metadata(Sql i2b2metadata){
		this.i2b2metadata = i2b2metadata
	}


	void setStudyName(String studyName){
		this.studyName = studyName
	}
	
	
	void setVisualAttrs(Map visualAttrs){
		this.visualAttrs = visualAttrs
	}
}

