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


class BioAssayPlatform {


	private static final Logger log = Logger.getLogger(BioAssayPlatform)

	Sql biomart
	String projectInfoTable
	
	/**
	 * 		pick records from PROJECT_INFO table and insert them into BIO_ASSAYA_PLATFORM 
	 * 		if they are not loaed yet
	 */
	void loadBioAssayPlatforms(){

		Map platform = [:]

		log.info "Start loading data from PROJECT_INFO into BIO_ASSAY_PLATFORM ..."

		String qry = """ select PROJECT_PLATFORM, PROJECT_PLATFORMDESCRIPTION,
						  	PROJECT_PLATFORMORGANISM, PROJECT_PLATFORMPROVIDER
						  from $projectInfoTable
						  where PROJECT_PLATFORM not in 
							 (select platform_name from bio_assay_platform)							
					  """

		biomart.eachRow(qry){
			
			String projectPlatform =  it.project_platform
			
			//java.sql.Clob pd = (java.sql.Clob) it.PROJECT_PLATFORMDESCRIPTION
			//String platformDescription = pd.getAsciiStream().getText()
			String platformDescription = it.PROJECT_PLATFORMDESCRIPTION

			String projectOrganism = it.PROJECT_PLATFORMORGANISM
			String projectProvider = it.PROJECT_PLATFORMPROVIDER

			
			if(projectPlatform.indexOf(";") != -1){

				String [] names = projectPlatform.split(";")
				
				String [] descriptions = [], organisms = [], providers = []
				
				if((!platformDescription.equals(null)) && (platformDescription.indexOf(";") != -1))  
					descriptions = platformDescription.split(";")
					
				if((!projectOrganism.equals(null)) && (projectOrganism.indexOf(";") != -1))  
					organisms = projectOrganism.split(";")
					
				if((!projectProvider.equals(null)) && (projectProvider.indexOf(";") != -1))  
					providers = projectProvider.split(";")

				if((names.size() == descriptions.size() || (descriptions.size() ==0))
				&& (names.size() == organisms.size() || (organisms.size() ==0))
				&& (names.size() == providers.size() || (providers.size() ==0))){
				
					String name = "", description = "", organism = "", provider = ""

					for(int i in 0..names.size()-1){
						name = names[i]

						if(descriptions.size() == 0) description = platformDescription
						if(organisms.size() == 0) organism = projectOrganism
						if(providers.size() == 0) provider = projectProvider

						if(descriptions.size() == names.size()) description =descriptions[i]
						if(organisms.size() == names.size()) organism = organisms[i]
						if(providers.size() == names.size()) provider = providers[i]

						platform["name"] = name
						platform["description"] = description
						platform["organism"] = organism
						platform["provider"] = provider
						println "$name \t $description \t $organism \t $provider"
						loadBioAssayPlatform(platform)
					}
				} else{
					// special case for HG-U133_Plus_2; GPL6400

					if(projectPlatform.indexOf("HG-U133_Plus_2; GPL6400") != -1){
						platform["name"] = names[0]
						platform["description"] = descriptions[0]
						platform["organism"] = organisms[0]
						platform["provider"] = providers[0]
						loadBioAssayPlatform(platform)

						platform["name"] = names[1]
						platform["description"] = descriptions[1] + ";" + descriptions[2]
						platform["organism"] = organisms[0]
						platform["provider"] = providers[1]
						loadBioAssayPlatform(platform)
					}
				}
			}else{
				platform["name"] = projectPlatform
				platform["description"] = platformDescription
				platform["organism"] = projectOrganism
				platform["provider"] = projectProvider
				loadBioAssayPlatform(platform)
			}
		}
	}


	void loadBioAssayPlatform(Map platform){

		String name = platform["name"].trim()
		String description = platform["description"]
		String organism = platform["organism"].trim()
		String provider = platform["provider"]

		String qry = """ insert into bio_assay_platform (platform_name, platform_description, platform_array,
								   platform_accession, platform_organism, platform_vendor)
						  values (?, ?, ?, ?, ?, ?) """

		if(isBioAssayPlatformExist(name)){
			log.warn("Platform ${platform["name"]} already exists.")
		} else{
			log.info "Adding platform: ${platform["name"]} ... "
			biomart.execute(qry,[
				name,
				description,
				name,
				name,
				organism,
				provider
			])
		}
	}
	
	
	void loadBioAssayPlatform2(Map platform){

		String name="", version="", description="", organism="", array="", accession="", type="", provider=""
		
		if(!platform["platform_name"].equals(null))  name = platform["platform_name"].trim()
		if(!platform["platform_version"].equals(null)) version = platform["platform_version"].trim()
		if(!platform["platform_description"].equals(null)) description = platform["platform_description"].trim()
		if(!platform["platform_organism"].equals(null)) organism = platform["platform_organism"].trim()
		if(!platform["platform_array"].equals(null)) array = platform["platform_array"]
		if(!platform["platform_accession"].equals(null)) accession = platform["platform_accession"]
		if(!platform["platform_type"].equals(null)) type = platform["platform_type"]
		if(!platform["platform_vendor"].equals(null)) provider = platform["platform_vendor"]

		String qry = """ insert into bio_assay_platform (platform_name, platform_version, platform_description, 
								platform_array, platform_accession, platform_organism, platform_vendor, platform_type)
						  values (?, ?, ?, ?, ?,  ?, ?, ?) """

		if(isBioAssayPlatformExist(name)){
			log.warn("Platform ${platform["platform_name"]} already exists.")
		} else{
			log.info "Adding platform: ${platform["platform_name"]} ... "
			biomart.execute(qry,[
				name,
				version,
				description,
				array,
				accession,
				organism,
				provider,
				type
			])
		}
	}


	boolean isBioAssayPlatformExist(String platformName){
		String qry = "select count(1) from bio_assay_platform where upper(trim(platform_name))=?"
		if(biomart.firstRow(qry, [platformName.toUpperCase()])[0] > 0){
			return true
		}else{
			return false
		}
	}


	void setProjectInfoTable(String projectInfoTable){
		this.projectInfoTable = projectInfoTable
	}


	void setBiomart(Sql biomart){
		this.biomart = biomart
	}
}
