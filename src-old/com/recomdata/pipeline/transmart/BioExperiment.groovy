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

class BioExperiment {

	private static final Logger log = Logger.getLogger(BioExperiment)

	Sql biomart
	String projectInfoTable, platform, GSEName, suffix

	void loadBioExperiments(){

		addColumns()

		log.info "Start loadng PROJECT_INFO into BIO_EXPERIMENT ... "

		String qry = """ INSERT INTO BIO_EXPERIMENT(BIO_EXPERIMENT_TYPE, 
								ETL_ID, 
                                ENTRYDT, 
                                ACCESSION,
								--DESCRIPTION, 
                                --DESIGN, 
                                PRIMARY_INVESTIGATOR,
                                OVERALL_DESIGN,
								CONTACT_FIELD,
								TITLE)
						 SELECT distinct 'Experiment',
								 'Omicsoft: ' || trim(upper(Project_Accession)),
								 sysdate,
								 trim(upper(Project_Accession)),
								 --substr(Project_Description,0,1990),
								-- substr(Project_Design, 0, 1990),
								 trim(Project_ContactCompany),
								 trim(project_platform),
								 trim(Project_ContactName),
								 trim(Project_Title)
						 FROM $projectInfoTable
						 WHERE upper(trim(Project_Accession)) not in
						    (select trim(upper(accession)) from bio_experiment) and Project_Accession is not null
					 """
		biomart.execute(qry)


		qry = """ update bio_experiment t 
				  set title=(select trim(Project_Title) from $projectInfoTable 
		 		  			 where Project_Accession=upper(t.accession) and Project_Title is not null)
		 		  where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment' 
						 and upper(accession) in (select project_accession from project_info);
			  """
		//biomart.execute(qry)

		qry = """ update bio_experiment t
			      set DESCRIPTION=(select SUBSTR(Project_Description,0,1990) from $projectInfoTable
			                       where Project_Accession = upper(t.accession)
			                             and Project_Description is not null)
			      where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment'
			           and upper(accession) in (select project_accession from $projectInfoTable)
			  """
		biomart.execute(qry)

		qry = """ update bio_experiment t
			 	  set DESIGN=(select substr(Project_Design, 0, 1990) from $projectInfoTable
			 		          where Project_Accession=upper(t.accession) 
			 					   and Project_Design is not null)
			      where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment' 
			            and upper(accession) in (select project_accession from $projectInfoTable)
			  """
		biomart.execute(qry)

		qry = """ update bio_experiment t
			 	  set PRIMARY_INVESTIGATOR=(select trim(Project_ContactCompany) from $projectInfoTable
				   							where Project_Accession=upper(t.accession) 
			 									  and Project_ContactCompany is not null)
			 	  where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment' 
			              and upper(accession) in (select project_accession from $projectInfoTable)
			  """
		//biomart.execute(qry)

		qry = """ update bio_experiment t
			 	  set OVERALL_DESIGN=(select trim(project_platform) from $projectInfoTable
				   					  where Project_Accession=upper(t.accession) 
			 		    					and project_platform is not null)
			      where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment' 
			 		  and upper(accession) in (select project_accession from $projectInfoTable)
			 """
		//biomart.execute(qry)

		qry = """ update bio_experiment t
			 	  set CONTACT_FIELD=(select trim(Project_ContactName) from project_info
			 	  where Project_Accession=upper(t.accession) 
			 			 and Project_ContactName is not null)
			  	  where etl_id like 'Omicsoft%' and BIO_EXPERIMENT_TYPE='Experiment' 
			 			 and upper(accession) in (select project_accession from project_info)
			 """
		//biomart.execute(qry)

		log.info "End loadng PROJECT_INFO into BIO_EXPERIMENT ... "
	}


	void loadBioExperiment(String gseNumber){

		addColumns()

		if(isBioExperiemtnExist(gseNumber)){
			log.warn "$gseNumber already loaded into BIO_EXPERIMENT."
		}else{
			log.info "Start creating a record into BIO_EXPERIMENT for $gseNumber ... "

			String qry = """ INSERT INTO BIO_EXPERIMENT(BIO_EXPERIMENT_TYPE, ETL_ID, ENTRYDT, ACCESSION)
							 SELECT distinct 'Experiment',
							 'Omicsoft: ' || trim(upper(Project_Accession)),
							 trim(upper(Project_Accession))
							 FROM PROJECT_INFO
							 WHERE upper(trim(Project_Accession)) not in
							 (select trim(upper(accession)) from bio_experiment) and Project_Accession is not null
						"""
		}
	}


	boolean isBioExperiemtnExist(String gseNumber){
		String qry = "select count(1) from bio_experiment where upper(trim(accession))=?"
		if(biomart.firstRow(qry, [gseNumber.toUpperCase()])[0] > 0){
			return true
		}else{
			return false
		}
	}


	void addColumns(){

		log.info "Add missing columns as needed ... "
		String [] columns = [
			"INSTITUTION:100",
			"COUNTRY:50",
			"BIOMARKER_TYPE:255",
			"TARGET:255",
			"ACCESS_TYPE:100"
		]

		columns.each{
			String colName = it.split(":")[0]
			int colSize = Integer.parseInt(it.split(":")[1])

			String qry = "select count(1) from user_tab_columns where table_name=? and column_name=?"
			if(!(biomart.firstRow(qry, ["BIO_EXPERIMENT", colName])[0] > 0)){
				qry = "alter table bio_experiment add $colName varchar2($colSize)"
				biomart.execute(qry)
			}
		}
	}

	void setProjectInfoTable(String projectInfoTable){
		this.projectInfoTable = projectInfoTable
	}


	void setTestsDataTable(String testsDataTable){
		this.testsDataTable = testsDataTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
