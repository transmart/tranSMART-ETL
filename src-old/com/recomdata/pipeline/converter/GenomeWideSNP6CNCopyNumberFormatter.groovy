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

import java.util.Properties;
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

class GenomeWideSNP6CNCopyNumberFormatter {

	private static final Logger log = Logger.getLogger(GenomeWideSNP6CNCopyNumberFormatter)

	private static Properties props

	Map snpCallMap, columnMap

	StringBuffer sb = new StringBuffer()

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		GenomeWideSNP6CNCopyNumberFormatter cnf = new GenomeWideSNP6CNCopyNumberFormatter()

		if(args.size() > 0){
			log.info("Start loading property files conf/Common.properties and ${args[0]} ...")
			cnf.setProperties(Util.loadConfiguration(args[0]));
		} else {
			log.info("Start loading property files conf/Common.properties and conf/SNP.properties ...")
			cnf.setProperties(Util.loadConfiguration("conf/SNP.properties"));
		}

		// output filename for CN copy number
		File cnOutput = new File(props.get("output_directory") + File.separator + props.get("cn_copy_number_output"))
		if(cnOutput.size() > 0){
			cnOutput.delete()
			cnOutput.createNewFile()
		}
		
		String copyNumberColumn = props.get("copy_number_coulmn")
		ArrayList al = new ArrayList()
		al = cnf.getCopyNumberFile(props)
		int count = 0
		al.each{
			count++
			println "Sample $count: $it"
			cnOutput.append(cnf.formatCopyNumberFile(it, copyNumberColumn).toString())
		}
	}


	ArrayList getCopyNumberFile(Properties props){

		ArrayList al = new ArrayList()

		String copyNumberFileDirectory = props.get("copy_number_source_directory")
		String copyNumberFilePattern = props.get("copy_number_file_pattern")

		log.info "Start looking Copy Number file for processing ..."
		log.info("Search CopyNumber file from " + copyNumberFileDirectory)

		File cn = new File(copyNumberFileDirectory)
		if(cn.isDirectory()){
			cn.eachFile{
				log.info "Checking file's pattern: " + it.toString()
				if(it.toString().indexOf(copyNumberFilePattern) > 0) {
					al.add(it)
				}
			}
		}

		return al
	}


	StringBuffer formatCopyNumberFile(File input, String copyNumberColumn){

		String sampleName = input.getName().split(/\./)[0]

		int copyNumberColumnIndex
		StringBuffer sb = new StringBuffer()

		int index = 0
		input.eachLine{
			if(it.indexOf("ProbeSetName") == 0) {
				println it
				columnMap = getColumnMap(it)

				if(copyNumberColumn.size() > 1) {
					copyNumberColumnIndex = columnMap[copyNumberColumn]
				}
				else {
					copyNumberColumnIndex = Integer.parseInt(copyNumberColumn)
				}
				log.info("Copy Number Column used: $copyNumberColumn[Column: $copyNumberColumnIndex]")
			}
			if(it.indexOf("CN_") == 0) {
				index++
				String [] str = it.split("\t")
				String line = sampleName + "\t" + str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[copyNumberColumnIndex] + "\n"
				sb.append(line)
			}
		}

		log.info("Total processed probes: " + index )

		return sb
	}


	Map getColumnMap (String header) {
		Map columnMap = [:]

		int index = 0
		header.split("\t").each{
			println index + ": \t" + it
			columnMap[it.replace(".CEL", "")] = index
			index++
		}

		return columnMap
	}


	void setProperties(Properties props){
		this.props = props
	}
}
