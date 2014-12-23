/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2012-2014 The TranSMART Foundation
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version, along with the following terms:
 *
 * 1.	You may convey a work based on this program in accordance with section 5,
 *      provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it,
 *      in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************/
  
package org.transmartproject.pipeline.omicsoft

import groovy.sql.Sql

import org.apache.log4j.Level
import org.apache.log4j.Logger;
 

class ProjectInfo {

	private static final Logger log = Logger.getLogger(ProjectInfo)

	Sql sql
	String projectInfoTable, projectInfoSuffix
	Map map = [:]


	ProjectInfo(Level logLevel){
		log.setLevel(logLevel)
	}


	void loadProjectInfo(File sourceDirectory){

		if(sourceDirectory.isDirectory()){
			sourceDirectory.eachFile {
				//if(it.toString().indexOf(".Project.txt") >0 ) {
				if(it.toString().indexOf(projectInfoSuffix) > -1 ) {
					loadProjectInfoFile(it)
				}
			}
		}else{
			loadProjectInfoFile(sourceDirectory)
		}
	}


	def loadProjectInfoFile(File file){
		String key

		String gseName = file.getName().split(/\./)[0].toUpperCase()

		if(isProjectInfoExist(gseName)){
			log.warn("$gseName already exists in $projectInfoTable")
		}else{
			log.info("Insert $gseName into $projectInfoTable ...")

			createProjectRecord(gseName, file.getName())
			HashSet columns = getColumns(projectInfoTable)

			String columnName
			file.eachLine{
				String [] str = getKeyValuePair(it, "	")
				if(!str.equals(null)){
					columnName = str[0].trim().replace(".", "_").toUpperCase()
					if(!columns.contains(columnName)) addColumn(columnName)
					updateColumn(columnName, str[1].trim(), gseName)
				}
			}
		}
	}


	void updateColumn(String columnName, String columnValue, String name){
		String qry = "update $projectInfoTable set $columnName =? where name=?"
		try{
			sql.execute(qry, [columnValue, name])
		} catch (Exception e){
			println e.getStackTrace()
		}
	}


	String [] getKeyValuePair(String line, String delimiter){
		String []  str = line.split(delimiter)
		if((str.size() != 2) || (str[0].trim().size() ==0) ||
		(str[1].trim().equals("."))){
			return null
		}else{
			return str
		}
	}


	void addColumn(String columnName){

		log.info "Add new column $columnName to table $projectInfoTable ..."

		String qry = "alter table $projectInfoTable add $columnName varchar2(4000)"
		sql.execute(qry)
	}


	HashSet getColumns(String tableName){
		HashSet hs = new HashSet()
		String qry = "select column_name from user_tab_columns where table_name=?"
		sql.eachRow(qry, [tableName]) {
			hs.add(it.column_name)
		}
		return hs
	}


	void createProjectRecord(String gseName, String fileName){
		String qry = "insert into $projectInfoTable (name, file_name) values(?, ?)"
		sql.execute(qry, [
			gseName,
			fileName
		])
	}


	boolean isProjectInfoExist(String name){
		String qry = "select count(1) from $projectInfoTable where name=?"
		if(sql.firstRow(qry, [name])[0] > 0) return true
		else return false
	}


	void setProjectInfoSuffix(String projectInfoSuffix){
		this.projectInfoSuffix = projectInfoSuffix
	}


	Sql setSql(Sql sql){
		this.sql = sql
	}


	void setProjectInfoTable(String projectInfoTable){
		this.projectInfoTable = projectInfoTable
	}

}
