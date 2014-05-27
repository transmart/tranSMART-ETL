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
  

package com.recomdata.pipeline.plink

import java.util.List;

import org.apache.log4j.Logger;

import com.recomdata.pipeline.converter.CopyNumberReader
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql


class SnpDataByProbe {

	private static final Logger log = Logger.getLogger(SnpDataByProbe)
	Runtime runtime = Runtime.getRuntime()

	Sql deapp
	CopyNumberReader copyNumberReader

	String prefix, path, chr, trial, cut
	Map copyNumberMap = [:]

	File mapFile, pedFile

	void loadSnpDataByProbeChromsome(String chr){

		setChromosome(chr)

		File map = getMapFile(chr)
		File ped = getPedFile(chr)

		if(!map.equals(null) && !ped.equals(null)) {

			setMapFile(map)
			setPedFile(ped)
			
			File cn = getCopyNumberFile(chr)
			if(cn.equals(null)){
				log.info "Start loading data without Copy Number ... "
				loadSnpDataByProbeWithoutCN()
			}else{
				log.info "Start loading data with Copy Number ... "
				copyNumberReader.setCopyNumberFile(cn)

				// patient_num:snp_id -> copy_number
				Map cnMap = copyNumberReader.copyNumberReader(chr)
				log.info(Util.getMemoryUsage(runtime))

				setCopyNumberMap(cnMap)
				log.info("Copy Number for Chromosome " + chr + ": " + cnMap.size())
				loadSnpDataByProbeWithCN()
				//loadSnpDataByProbeWithCN1()
			}
		}
	}


	/**
	 *  parse PLINK data files (.PED and .MAP) for each chromosome, and 
	 *  then load them into DE_SNP_DATA_BY_PROBE
	 *   
	 * @return
	 */	
	def loadSnpDataByProbeWithoutCN(){

		log.info "Start loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."

		File pedFile = getPedFile(chr)
		List snpList = getMapData(chr)

		Map snpMap = [:]
		for(i in 0 .. snpList.size()-1){
			snpMap[snpList[i]] = " "
		}

		log.info "Loop through PED file: " + pedFile.toString()

		def index = 0
		pedFile.eachLine {
			String [] str = it.split(" +")

			for(i in 6 .. str.size()-1){
				if(i % 2 == 0) {
					int m = (i-6)/2
					String rs = snpList[m]
					snpMap[rs] += str[i]
				} else  {
					int m = (i-7)/2
					String rs = snpList[m]
					snpMap[rs] += str[i] + " "
				}
			}
			index++
		}

		Map snpIdMap = getSnpIdMapByChr()
		snpMap.each() { key, value ->
			if(snpIdMap[key]){
				String [] str =  snpIdMap[key].split(":")
				Map dataMap = [:]
				dataMap["probeId"] = str[1]
				dataMap["probename"] = key
				dataMap["snpId"] = str[0]
				dataMap["snpName"] = str[2]
				dataMap["trialName"] = trial
				dataMap["dataByProbe"] = value
				loadSnpDataByProbe(dataMap)
			}else{
				log.info "No mapping info for " + key + " in Chromosome " + chr
			}
		}
		log.info "End loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."
	}

	
	def loadSnpDataByProbeWithoutCN1(){

		log.info "Start loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."

		File pedFile = getPedFile(chr)

		// [index] -> snp_id (the 2nd column of *.map file
		List snpList = getMapData(chr)

		// name -> snp_info_id : snp_probe_id : snp_name
		Map snpIdMap = getSnpIdMapByChr()

		StringBuffer sb = new StringBuffer()
		int index1, index2
		String [] str

		/*
		 *  snp_id -> snp_infor_id (de_snp_info)
		 *  probe_name -> name (de_snp_info)
		 *  probe_id -> snp_probe_id (de_snp_probe)
		 *  snp_name -> snp_name (de_snp_probe)
		 */
		String qry = """ insert into de_snp_data_by_probe (probe_id, probe_name, snp_id,
							   snp_name, trial_name, data_by_probe)
						values(?, ?, ?, ?, ?, ?) """

		deapp.withTransaction {
			deapp.withBatch(1000, qry, { stmt ->
				for(i in 0..snpList.size()-1){
					
					if(i % 1000 ==0){
						print i + "   "
					}
					
					index1 = 7 + 2*i
					index2 = 8 + 2*i

					def process = "$cut -d\" \" -f$index1,$index2 $pedFile".execute()
					process.in.eachLine{
						sb.append(it.replace(" ", "") + " ")
					}

					if(snpIdMap[snpList[i]]){
						str =  snpIdMap[snpList[i]].split(":")
						stmt.addBatch([
							str[1],
							snpList[i],
							str[0],
							str[2],
							trial,
							sb.toString()
						])
					}
					sb.setLength(0)
				}
			})
		}

		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		log.info "End loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."
	}

	

	def loadSnpDataByProbeWithCN(){

		log.info "Start loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."

		//File pedFile = getPedFile(chr)

		List snpList = getMapData(chr)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		Map snpMap = [:]
		for(i in 0 .. snpList.size()-1){
			snpMap[snpList[i]] = " "
		}
		log.info(Util.getMemoryUsage(runtime))

		log.info "Start loop through PED file: " + pedFile.toString()

		def index = 0
		pedFile.eachLine {
			String [] str = it.split(" +")

			List pedLineData = getPedDataByLine(it)

			if(snpList.size() == pedLineData.size()){
				for(i in 0..snpList.size()-1){
					String key = str[0] + ":" + snpList[i]
					if(copyNumberMap[key].equals(null)) snpMap[snpList[i]] += pedLineData[i] + " "
					else snpMap[snpList[i]] += copyNumberMap[key] + pedLineData[i] + " "
				}
			}else{
				log.error("SNPs in MAP file did match to columns in PED file:" + pedFile.toString())
			}
			index++
		}

		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())
		log.info "End loop through PED file: " + pedFile.toString()

		Map snpIdMap = getSnpIdMapByChr()
		snpMap.each() { key, value ->
			if(snpIdMap[key]){
				String [] str =  snpIdMap[key].split(":")
				Map dataMap = [:]
				dataMap["probeId"] = str[1]
				dataMap["probeName"] = key
				dataMap["snpId"] = str[0]
				dataMap["snpName"] = str[2]
				dataMap["trialName"] = trial
				dataMap["dataByProbe"] = value
				loadSnpDataByProbe(dataMap)
			}else{
				log.info "No mapping info for " + key + " in Chromosome " + chr
			}
		}
		log.info "End loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."
	}


	def loadSnpDataByProbeWithCN1(){

		log.info "Start loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."

		File pedFile = getPedFile(chr)

		// [index] -> snp_id (the 2nd column of *.map file
		List snpList = getMapData(chr)
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		// name -> snp_info_id : snp_probe_id : snp_name
		Map snpIdMap = getSnpIdMapByChr()
		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		StringBuffer sb = new StringBuffer()
		int index1, index2
		String [] str, line
		String key

		/*
		 *  snp_id -> snp_infor_id (de_snp_info)
		 *  probe_name -> name (de_snp_info)
		 *  probe_id -> snp_probe_id (de_snp_probe)
		 *  snp_name -> snp_name (de_snp_probe)
		 */
		String qry = """ insert into de_snp_data_by_probe (probe_id, probe_name, snp_id,
							   snp_name, trial_name, data_by_probe)
						values(?, ?, ?, ?, ?, ?) """

		//deapp.withTransaction {
			deapp.withBatch(100, qry, { stmt ->
				for(i in 0..snpList.size()-1){
					index1 = 7 + 2*i
					index2 = 8 + 2*i
					
					if(i % 1000 == 0) print i + "   "

					def process = "$cut -d\" \" -f1,$index1,$index2 $pedFile".execute()
					process.in.eachLine{
						line = it.split(" ")
						key = line[0] + ":" + snpList[i]
						
						if(copyNumberMap[key].equals(null))
							sb.append(line[1] + line[2] + " ")
						else
							sb.append(copyNumberMap[key] + line[1] + line[2] + " ")
					}

					if(snpIdMap[snpList[i]]){
						str =  snpIdMap[snpList[i]].split(":")
						stmt.addBatch([
							str[1],
							snpList[i],
							str[0],
							str[2],
							trial,
							sb.toString()
						])
					}
					sb.setLength(0)
				}
			})
		//}

		log.info(Util.getMemoryUsage(runtime))
		log.info(new Date())

		log.info "End loading DE_SNP_DATA_BY_PROBE for Chromosome $chr ..."
	}


	void loadSnpDataByProbe(Map dataMap){

		String qry = """ insert into de_snp_data_by_probe (probe_id, probe_name, snp_id,
									snp_name, trial_name, data_by_probe)
						 values(?, ?, ?, ?, ?, ?) """

		int probeId = Integer.parseInt(dataMap["probeId"])
		String probeName = dataMap["probeName"]

		int snpId = Integer.parseInt(dataMap["snpId"])
		String snpName = dataMap["snpName"]

		String trialName = dataMap["trialName"]
		String dataByProbe = dataMap["dataByProbe"].toString()

		// comment out because of performance issue
		//if(isProbeExist(str[0]) == 0)
		if(probeId == 0){
			deapp.execute(qry, [
				null,
				"",
				snpId,
				snpName,
				trialName,
				dataByProbe
			])
		}else{
			deapp.execute(qry, [
				probeId,
				probeName,
				snpId,
				snpName,
				trialName,
				dataByProbe
			])
		}
	}


	/**
	 *   extract all SNPs for a particular chromosome and form a map used to 
	 *   create data for DATA_BY_PROBE column
	 *   
	 * @return
	 */
	Map getSnpIdMapByChr(){

		log.info "Retrieve mapping info for SNP ID and RS# ..."

		Map snpIdMap = [:]

		/*
		 *  RS# stored in de_snp_probe and SNP_ID stored in de_snp_info
		 *  otherwise SNPs w/o RS# will be lost in the following analysis
		 *    de_snp_info.name: SNP_ID
		 */
		String qry = """ select t1.name, t1.snp_info_id, t2.snp_name, t2.snp_probe_id 
		                 from de_snp_info t1, de_snp_probe t2
		                 where t1.chrom = ? and t1.snp_info_id=t2.snp_id(+) """

		deapp.eachRow(qry, [chr]) {
			if(it.snp_name.equals(null)) {
				log.info it.name + " in Chromosome " + chr + " without RS# "
				snpIdMap[it.name] = it.snp_info_id + ":0:NA"
			}
			else{
				snpIdMap[it.name] = it.snp_info_id + ":" + it.snp_probe_id + ":" + it.snp_name
			}
		}
		return snpIdMap
	}



	List getMapData(String chr){

		//File mapFile = getMapFile(chr)

		List map = []
		log.info "Loading MAP data from: " + mapFile.toString()
		int index = 0
		mapFile.eachLine{
			String [] str = it.split("\t")
			map[index] = str[1]
			index++
		}
		log.info("Total SNPs in " + mapFile.toString() + ":  " + map.size())
		return map
	}


	List getPedDataByLine(String line){

		List pedLineData = []

		String [] str = line.split(" +")
		for(i in 6 .. str.size()-1){
			if(i % 2 == 0) {
				int m = (i-6)/2
				pedLineData[m] = str[i]
			} else  {
				int m = (i-7)/2
				pedLineData[m] += str[i]
			}
		}

		return pedLineData
	}


	File getPedFile(String chr){
		File pedFile = new File(path + "/" + prefix + chr + ".ped")

		if(pedFile.exists()) {
			log.info "Looking for PED file: " + pedFile.toString()
			return pedFile
		}
		else{
			log.error "Cannot find PED file: " + pedFile.toString()
			return null
		}
	}


	File getMapFile(String chr){
		File mapFile = new File(path + "/" + prefix + chr + ".map")

		if(mapFile.exists()) {
			log.info "Looking for MAP file: " + mapFile.toString()
			return mapFile
		}
		else{
			log.error "Cannot find MAP file: " + mapFile.toString()
			return null
		}
	}


	File getCopyNumberFile(String chr){
		File cn = new File(path + "/" + prefix + chr + ".cn")
		if(cn.exists()){
			log.info "Looking for Copy Number file: " + cn.toString()
			return cn
		} else{
			log.warn("Cannot find Copy Number file: " + cn.toString())
			return null
		}
	}


	/**
	 *   try to figure out if a SNP for this particular study is loaded,
	 *   if not, it will return o, otherwise 1.
	 *   
	 * @param snpId 	SNP Id from DE_SNP_INFO
	 * @return
	 */
	int isProbeExist(String snpId){
		String qry = """ select count(1) from de_snp_data_by_probe 
		                 where trial_name = ? and snp_id = ? """
		def res = deapp.firstRow(qry, [trial, snpId])
		return res[0]
	}


	/**
	 * 
	 * @param path
	 * @return
	 */
	void setPath(String path){
		this.path = path
	}

	/**
	 * 
	 * @param prefix
	 * @return
	 */
	void setPrefix(String prefix){
		this.prefix = prefix
	}

	/**
	 * 
	 * @param trial
	 * @return
	 */
	void setTrial(String trial){
		this.trial = trial
	}


	void setCut(String cut){
		this.cut = cut
	}

	
	void setMapFile(File mapFile){
		this.mapFile = mapFile
	}

	
	void setPedFile(File pedFile){
		this.pedFile = pedFile
	}
	
	
	/**
	 * 
	 * @param chr
	 * @return
	 */
	void setChromosome(String chr){
		this.chr = chr
	}


	/**
	 * 
	 * @param deapp
	 * @return
	 */	
	void setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}


	void setCopyNumberMap(Map copyNumberMap){
		this.copyNumberMap = copyNumberMap
	}

	void setCopyNumberReader(CopyNumberReader copyNumberReader){
		this.copyNumberReader = copyNumberReader
	}
}
