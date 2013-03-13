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
  

package com.recomdata.pipeline.annotation

import java.util.Properties;

import groovy.sql.Sql
import org.apache.log4j.Logger

import org.apache.log4j.PropertyConfigurator
import groovy.sql.Sql
import com.recomdata.pipeline.util.Util

class GPLReader {

	private static final Logger log = Logger.getLogger(GPLReader)	
	
	String  sourceDirectory
	Sql sql
	Map expectedProbes
	File snpInfo, probeInfo, snpGeneMap, gplInput, snpMap

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()
		GPLReader al = new GPLReader()

		Properties props = Util.loadConfiguration("conf/loader.properties")

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

		al.loadGxGPL(props, biomart)
	}

	
	void loadGxGPL(Properties props, Sql biomart){

		if(props.get("skip_gx_gpl_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading GX GPL annotation file(s) ..."
		}else{


			GexGPL gpl = new GexGPL()
			gpl.setSql(biomart)
			gpl.setAnnotationTable(props.get("annotation_table"))

			if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating annotation table ${props.get("annotation_table")} for GPL GX annotation file(s) ..."
				gpl.createAnnotationTable()
			}


			String annotationSourceDirectory = props.get("annotation_source")
			String [] gplList = props.get("gpl_list").split(/\,/)
			gplList.each {
				File annotationSource = new File(annotationSourceDirectory + File.separator + "GPL." + it + ".txt")
				gpl.loadGxGPLs(annotationSource)
			}
		}
	}

	void processGPLs(String inputFileName){

		long numProbes
		if(inputFileName.indexOf(";")){
			String [] names = inputFileName.split(";")
			for(int i in 0..names.size()-1){
				File inputFile = new File(sourceDirectory + File.separator + names[i])

				if(inputFile.exists()){
					log.info("Start parsing " + inputFile.toString())

					setGPLInputFile(inputFile)
					numProbes = parseGPLFile() //probeInfo, snpGeneMap, snpMapFile)
					if(numProbes == expectedProbes[names[i]])
						log.info("Probes in " + names[i] + ": " + numProbes + "; expected: " + expectedProbes[names[i]])
					else
						log.warn("Probes in " + names[i] + ": " + numProbes + "; expected: " + expectedProbes[names[i]])
				}else{
					log.warn("Cannot find the file: " + inputFile.toString())
				}
			}
		}else{
			File inputFile = new File(sourceDirectory + File.separator + inputFileName)

			log.info("Start parsing " + inputFile.toString())

			setGPLInputFile(inputFile)
			numProbes = parseGPLFile() //probeInfo, snpGeneMap, snpMapFile)
			if(numProbes == expectedProbes[inputFileName])
				log.info("Probes in " + inputFileName + ": " + numProbes + "; expected: " + expectedProbes[inputFileName])
			else
				log.warn("Probes in " + inputFileName + ": " + numProbes + "; expected: " + expectedProbes[inputFileName])
		}
	}


	long parseGPLFile(){
		String [] str, header
		def genes = [:]
		boolean isHeaderLine = false
		boolean isAnnotationLine = false

		StringBuffer sb_probeinfo = new StringBuffer()
		StringBuffer sb_snpGeneMap = new StringBuffer()
		StringBuffer sb_snpMap = new StringBuffer()

		long numProbes = 0
		gplInput.eachLine{
			str = it.split("\t")
			if(str.size() > 14){
				if(it.indexOf("ID") == 0) {
					isHeaderLine = true

					if((gplInput.name.indexOf("GPL2004") > -1) || (gplInput.name.indexOf("GPL2005") > -1)){
						// used for GPL2004 and GPL2005
						log.info str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[5] + "\t" + str[12]
					}

					if((gplInput.name.indexOf("GPL3718") > -1) || (gplInput.name.indexOf("GPL3720") > -1)){
						// used for GPL3718 and GPL3720
						log.info str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[5] + "\t" + str[13]
					}
				}else{
					//if(isHeaderLine && numProbes < 10) {
					if(isHeaderLine) {
						String snpId, rsId, chr, pos
						if((gplInput.name.indexOf("GPL2004") > -1) || (gplInput.name.indexOf("GPL2005") > -1)){
							// used for GPL2004 and GPL2005
							snpId = str[0].trim()
							rsId = str[1].trim()
							chr = str[2].trim()
							pos = str[5].trim()
							genes = getSNPGeneMapping(str[12])
						}

						if((gplInput.name.indexOf("GPL3718") > -1) || (gplInput.name.indexOf("GPL3720") > -1)){
							// used for GPL3718 and GPL3720
							snpId = str[0].trim()
							rsId = str[2].trim()
							chr = str[3].trim()
							pos = str[6].trim()
							genes = getSNPGeneMapping(str[13])

						}

						if(!chr.equals(null) && !pos.equals(null) && (chr.size()>0) && (pos.size() > 0) &&
						!(snpId.indexOf("AFFX") >= 0)){
							numProbes++
							if(!rsId.equals(null) && (rsId.indexOf("---") == -1))  sb_probeinfo.append(snpId + "\t" + rsId + "\n")
							sb_snpMap.append(chr + "\t" + snpId + "\t0\t" + pos + "\n")
						}

						genes.each { key, val ->
							String [] s = val.split(":")
							String mappingRecord = str[0] + "\t" + key
							sb_snpGeneMap.append(mappingRecord + "\n")
						}
					}
				}
			}
		}

		probeInfo.append(sb_probeinfo.toString())
		snpGeneMap.append(sb_snpGeneMap.toString())
		snpMap.append(sb_snpMap.toString())

		return numProbes
	}


	Map getSNPGeneMapping(String associatedGene){

		String [] str, gene
		def mapping = [:]

		if(associatedGene.indexOf("///") >= 0) {
			str = associatedGene.split("///")
			for(int i in 0..str.size()-1) {

				if(str[i].indexOf("//")) {
					gene = str[i].split("//")
					if(gene.size() >= 6 &&  !(gene[5].indexOf("---") >= 0)){
						// 4 -- gene symbol; 5 -- gene id; 6 -- gene description
						//println gene[4] + ":" + gene[5] + ":" + gene[6]
						mapping[gene[5].trim()] = gene[4].trim() + ":" + gene[6].trim()
					}
				}
			}
		}
		return mapping
	}


	void setProbeInfo(File probeInfo){
		this.probeInfo = probeInfo
	}

	void setSnpGeneMap(File snpGeneMap){
		this.snpGeneMap = snpGeneMap
	}


	void setSnpMap(File snpMap){
		this.snpMap = snpMap
	}


	void setGPLInputFile(File gplInput){
		this.gplInput = gplInput
	}


	void setSourceDirectory(String sourceDirectory){
		this.sourceDirectory = sourceDirectory
	}

	void setExpectedProbes(Map expectedProbes){
		this.expectedProbes = expectedProbes
	}

	void setSql(Sql sql){
		this.sql = sql
	}
}
