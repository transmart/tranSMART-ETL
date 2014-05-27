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
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

class GenomeWideSNP6GenotypeFormatter {

	private static final Logger log = Logger.getLogger(GenomeWideSNP6GenotypeFormatter)

	Map snpCallMap, columnMap

	StringBuffer sb = new StringBuffer()

	static main(args) {

		log.info("Start loading property file SNP.properties ...")
		Properties props = Util.loadConfiguration("conf/SNP.properties");

		GenomeWideSNP6GenotypeFormatter gtf = new GenomeWideSNP6GenotypeFormatter()
		File input = new File(props.get("genotype_source_file"))
		File output = gtf.createGenotypeOutputFile(props)
		int bufferSize = Integer.parseInt(props.get("buffer_size"))
		gtf.formatGenotypeData(input, output, bufferSize)
	}


	void formatGenotypeData(File input, File output, int bufferSize){

		StringBuffer sbGenotype = new StringBuffer()
		int newLine = 10*bufferSize

		int index = 0
		input.eachLine{
			if(it.indexOf("#Calls:") == 0) {
				println it
				snpCallMap = getSnpCallMap(it.replace("#Calls:", ""))
			}
			if(it.indexOf("probeset_id") == 0) {
				println it
				columnMap = getColumnMap(it)
			}
			if(it.indexOf("SNP_A-") == 0) {
				index++
				sbGenotype.append(extractLineData(it).toString())

				if((index % bufferSize) == 0) {
					if((index % newLine) == 0) println index + "..."
					else print index + "..."
					output.append(sbGenotype.toString())
					sbGenotype.setLength(0)
				}
			}
		}
		output.append(sbGenotype.toString())
		sbGenotype.setLength(0)
		log.info("\nTotal processed probes: " + index )
	}


	StringBuffer extractLineData(String line){

		StringBuffer sb = new StringBuffer()

		String [] snpCalls = line.split("\t")
		for(int i=1; i<snpCalls.size(); i++){
			sb.append(columnMap[i] + "\t")
			sb.append(snpCalls[0] + "\t")
			String genotype = snpCallMap[snpCalls[i]]
			sb.append(" " + genotype.substring(0,1)  + " " + genotype.substring(1) + "\n")
		}

		return sb
	}


	Map getColumnMap (String header) {
		Map columnMap = [:]

		int index = 0
		header.split("\t").each{
			println index + ": \t" + it
			columnMap[index] = it.replace(".CEL", "")
			index++
		}

		return columnMap
	}


	Map getSnpCallMap(String str){

		Map snpCallMap = [:]

		str.split(",").each{
			def call = it.split("=")
			// replace missing genotype: "NN" to "00" 
			snpCallMap[call[0].trim()] = call[1].trim().replace("NN", "00")
		}

		snpCallMap.each{ k, v -> println k + ": \t \"" + v}
		return snpCallMap
	}


	File createGenotypeOutputFile(Properties props){
		File gt = new File(props.get("output_directory") + File.separator + props.get("study_name") + ".genotype")
		if(gt.size() > 0){
			gt.delete()
		}
		gt.createNewFile()

		return gt
	}
}
