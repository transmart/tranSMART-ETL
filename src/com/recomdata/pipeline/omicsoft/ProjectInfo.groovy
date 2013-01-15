package com.recomdata.pipeline.omicsoft

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
