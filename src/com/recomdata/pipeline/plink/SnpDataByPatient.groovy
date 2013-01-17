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

import org.apache.log4j.Logger;

import com.recomdata.pipeline.converter.CopyNumberReader
import com.recomdata.pipeline.i2b2.PatientDimension;
import com.recomdata.pipeline.util.Util

import groovy.sql.Sql

class SnpDataByPatient {

	private static final Logger log = Logger.getLogger(SnpDataByPatient)

	Sql deapp
	String trial, prefix, path, sourceSystemPrefix
	Map sampleType, subjectSnpDatasetMap

	Map <String, StringBuffer> pedByPatientAll = [:], dataByPatientAll = [:]
	Map pedByDataset, dataByDataset

	PatientDimension patientDimension
	SubjectSnpDataset ssd
	CopyNumberReader copyNumberReader

	File mapFile, pedFile, cnFile, pedByPatientAllFile, dataByPatientAllFile


	/**
	 *  parse and load a PED file for a particular chromosome under the specified directory.
	 *  The name of PED file should be in the format of PREFIX+chr.ped, usually PREFIX=chr
	 *  and chr is in 1 - 26, such as chr26.ped for Chromosome MT.  
	 *   
	 * @param pedPath		directory stored PED files
	 * @param chr			chromosome ( 1 - 26)
	 * @return
	 */
	def loadPatientSnpDataFromPed(String pedPath, String chr){

		log.info "Start loaing SNP data for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."

		String qry = """ insert into de_snp_data_by_patient (snp_dataset_id, 
		                   trial_name, patient_num, chrom, 
		                   data_by_patient_chr, ped_by_patient_chr)
		                 values(?, ?, ?, ?, ?, ?) """

		pedFile.eachLine {

			String [] str = it.split(" +")
			String indId = cleanupId(str[1].trim())
			String subjectId = str[0].trim() + ":" + indId

			//def patientNum = pd.getPatientNumberByIndividualId(indId)
			//def patientNum = Integer.parseInt(str[0])
			int patientNum = 0

			if(patientDimension.isPatientNumber(indId)){
				patientNum = Integer.parseInt(indId)
			}else{
				String individualId = sourceSystemPrefix + ":" + indId
				long pid = patientDimension.getPatientNumberByIndividualId(individualId)
				if(pid > 0){
					patientNum = pid
				}else{
					log.error("Cannot find patient_number for: " + subjectId)
					throw new RuntimeException("Cannot find patient_number for: " + subjectId)
				}
			}

			def datasetId = ssd.getSnpDatasetId(trial, subjectId)

			StringBuffer data1 = getPedByPatientChr(it)
			StringBuffer data2 = getDataByPatientChr(it)


			if(datasetId != null){
				if(!isPatientSnpDataExist(datasetId, chr))
					deapp.execute(qry, [
						datasetId,
						trial,
						patientNum,
						chr,
						data2.toString(),
						data1.toString()
					])
			}
		}
		log.info "End loaing SNP data for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."
	}


	/**
	 * load SNP's genotyping data with Copy Number data
	 * 
	 * @param chr
	 * @param cnMap
	 * @return
	 */
	def loadPatientSnpDataFromPedWithCN(String chr, Map cnMap){

		log.info "Start loaing SNP data with Copy Number for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."

		List map = getMapData()

		Map dataMap = [:]

		pedFile.eachLine {
			String [] str = it.split(" +")

			long patientNum = Long.parseLong(str[0])
			dataMap["patientNum"] = patientNum

			dataMap["trial"] = trial
			dataMap["chr"] = chr.toUpperCase()

			long datasetId = subjectSnpDatasetMap[patientNum.toBigDecimal()]
			dataMap["datasetId"] = datasetId

			dataMap["pedByPatient"] = getPedByPatientChr(it).toString()
			dataMap["dataByPatient"] = getDataByPatientChr(it, cnMap, map).toString()

			if(datasetId != null){
				if(!isPatientSnpDataExist(datasetId, chr))
					loadPatientSnpData(dataMap)
			}
		}
		log.info "End loaing SNP data for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."
	}


	/**
	 * load SNP's genotyping data without Copy Number data
	 * 	
	 * @param chr
	 * @return
	 */
	def loadPatientSnpDataFromPed(String chr){

		log.info "Start loaing SNP data for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."

		Map dataMap = [:]

		pedFile.eachLine {
			String [] str = it.split(" +")

			long patientNum = Long.parseLong(str[0])
			dataMap["patientNum"] = patientNum

			dataMap["trial"] = trial
			dataMap["chr"] = chr.toUpperCase()

			long datasetId = subjectSnpDatasetMap[patientNum.toBigDecimal()]
			dataMap["datasetId"] = datasetId

			dataMap["pedByPatient"] = getPedByPatientChr(it).toString()
			dataMap["dataByPatient"] = getDataByPatientChr(it).toString()
			if(datasetId != null){
				if(!isPatientSnpDataExist(datasetId, chr))
					loadPatientSnpData(dataMap)
			}
		}

		log.info "End loaing SNP data for Chromosome $chr into DE_SNP_DATA_BY_PATIENT ..."
	}


	void loadSnpDataByPatientChromosomes(List chrs){

		// ignore chr25 (XY) and chr26 (MT) for now
		chrs.each{ chr -> loadSnpDataByPatientChromosome(chr) }
	}


	void loadSnpDataByPatientChromosome(String chr){

		File map = new File(path + "/" + prefix + chr + ".map")
		if(!map.exists()) {
			log.warn "Cannot find MAP file: " + map.toString()
		}

		File ped = new File(path + "/" + prefix + chr + ".ped")
		if(!ped.exists()) {
			log.warn "Cannot find PED file: " + ped.toString()
		}

		File cn = new File(path + "/" + prefix + chr + ".cn")

		if(map.exists() && ped.exists()) {
			setPedFile(ped)
			log.info "Loading PED file: " + ped.toString()

			setMapFile(map)
			log.info "Loading Map file: " + map.toString()

			if(cn.exists()){
				log.info "Loading CN file: " + cn.toString()
				copyNumberReader.setCopyNumberFile(cn)
				Map cnMap = copyNumberReader.copyNumberReader(chr)
				log.info("Copy Number records for chr$chr: " + cnMap.size())
				loadPatientSnpDataFromPedWithCN(chr, cnMap)
			} else{
				log.warn("Cannot find Copy Number file: " + cn.toString())
				log.warn("Loading SNP data without Copy Number for Chromosome: " + chr)
				loadPatientSnpDataFromPed(chr)
			}
		}
	}


	void loadSnpDataByPatientAllChromosome(){

		log.info "Start loading data for ALL chromsomes per patient ..."

		Map dataMap = [:]

		pedByPatientAll.each{k, v ->
			String [] str = k.split(":")

			long patientNum = Integer.parseInt(str[0])
			dataMap["patientNum"] = patientNum
			dataMap["trial"] = trial
			dataMap["chr"] = "ALL"

			long datasetId = subjectSnpDatasetMap[patientNum.toBigDecimal()]
			dataMap["datasetId"] = datasetId

			dataMap["pedByPatient"] = v.toString()
			dataMap["dataByPatient"] = dataByPatientAll[k].toString()

			if(datasetId != null){
				if(!isPatientSnpDataExist(datasetId, "ALL")) loadPatientSnpData(dataMap)
			}
		}
		
		log.info "End loading data for ALL chromsomes per patient ..."
	}



	void loadSnpDataByPatientAllChromosomes(){
		subjectSnpDatasetMap.each { patientNum, datasetId ->
			loadSnpDataByPatientAllChromosome(datasetId.toString(), patientNum.toString())
		}
	}


	void loadSnpDataByPatientAllChromosome(String datasetId, String patientNum){

		Map dataMap = [:]
		dataMap["patientNum"] = patientNum
		dataMap["datasetId"] = datasetId
		dataMap["trial"] = trial
		dataMap["chr"] = "ALL"

		getSnpDataByPatientDataset(datasetId)

		StringBuffer ped = new StringBuffer()
		StringBuffer data = new StringBuffer()

		for(i in 1..24){
			ped.append(pedByDataset[i.toString()])
			data.append(dataByDataset[i.toString()])
		}

		dataMap["pedByPatient"] = ped.toString()
		dataMap["dataByPatient"] = data.toString()

		if(!isPatientSnpDataExist(Long.parseLong(datasetId), "ALL")) {
			log.info "Start loading data for ALL chromsomes for patient:dataset ->" + datasetId + ":" + patientNum
			loadPatientSnpData(dataMap)
			log.info "End loading data for ALL chromsomes for patient:dataset ->" + datasetId + ":" + patientNum
		}
	}


	void getSnpDataByPatientDataset(String datasetId)	{

		pedByDataset = [:]
		dataByDataset = [:]

		String qry = """select chrom, data_by_patient_chr, ped_by_patient_chr  
			                from de_snp_data_by_patient  
			                where snp_dataset_id = ?"""
		deapp.eachRow(qry, [datasetId]){
			java.sql.Clob clob_ped = (java.sql.Clob) it.ped_by_patient_chr
			java.sql.Clob clob_data = (java.sql.Clob) it.data_by_patient_chr

			//pedByDataset[it.chrom] = clob_ped.getAsciiStream().getText()
			//dataByDataset[it.chrom] = clob_ped.getAsciiStream().getText()
			pedByDataset[it.chrom] = clob_ped.getCharacterStream().getText()
			dataByDataset[it.chrom] = clob_data.getCharacterStream().getText()
		}
	}


	/**
	 * check if a record already exists in DE_SNP_DATA_BY_PATIENT
	 *  
	 * @param datasetId
	 * @param chr
	 * @return			
	 */
	boolean isPatientSnpDataExist(long datasetId, String chr){

		String qry = """ select count(1) from de_snp_data_by_patient
						 where snp_dataset_id = ? and chrom= ? """

		def cnt = deapp.firstRow(qry, [datasetId, chr])

		if(cnt[0] == 0) return false
		else return true
	}


	/**
	 * insert into DE_SNP_DATA_BY_PATIENT
	 * 
	 * @param dataMap
	 */
	void loadPatientSnpData(Map dataMap){

		log.info "Start inserting SNP data for (${dataMap["chr"]}, ${dataMap["patientNum"]}, ${dataMap["datasetId"]}) into DE_SNP_DATA_BY_PATIENT ..."

		String qry = """ insert into de_snp_data_by_patient (snp_dataset_id,
								   trial_name, patient_num, chrom,
								   data_by_patient_chr, ped_by_patient_chr)
						 values(?, ?, ?, ?, ?, ?) """

		deapp.execute(qry, [
			dataMap["datasetId"],
			dataMap["trial"],
			dataMap["patientNum"],
			dataMap["chr"],
			dataMap["dataByPatient"],
			dataMap["pedByPatient"]
		])

		log.info "Start inserting SNP data for (${dataMap["chr"]}, ${dataMap["patientNum"]}, ${dataMap["datasetId"]}) into DE_SNP_DATA_BY_PATIENT ..."
	}


	/**
	 *  combine Copy Number with Genotyping data and format it for DATA_BY_PATIENT_CHR
	 *  column in DE_SNP_DATA_BY_PATIENT
	 *  
	 * @param line		line of PED file
	 * @param cnMap		Copy Number data, patientNum:SNPId -> CN
	 * @param map		List of SNP Id in MAP's order
	 * @return			value for DATA_BY_PATIENT_CHR
	 */
	StringBuffer getDataByPatientChr(String line, Map cnMap, List map){

		int index = 0
		StringBuffer sb = new StringBuffer()
		String [] str = line.split(" +")
		for(i in 6 .. str.size()-1){
			if(i % 2 ==0){
				String snpId = map[index]
				String key = str[0] + ":" + snpId
				if(!cnMap[key].equals(null))  sb.append(cnMap[key] + "\t" + str[i])
				else sb.append("\t" + str[i])
				index++
			}
			else sb.append(str[i] + "\n")
		}
		return sb
	}



	/**
	 *  parse a line from PED file and form a record for PED_BY_PATIENT_CHR column
	 *  in DE_SNP_DATA_BY_PATIENT, and this column is used for PLINK
	 *
	 * @param line		line of PED file
	 * @return
	 */
	StringBuffer getPedByPatientChr(String line){

		StringBuffer sb = new StringBuffer()
		String [] str = line.split(" +")
		for(i in 6 .. str.size()-1){
			sb.append(str[i] + " ")
		}
		return sb
	}


	List getMapData(){

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


	Map getPatientDatasetMap(){

		log.info "Get patient_number: dataset_id map from de_snp_data_by_patient"
		Map datasetMap = [:]
		String qry = "select distinct patient_num, snp_dataset_id from de_snp_data_by_patient"
		deapp.eachRow(qry) {
			datasetMap[it.snp_dataset_id] = it.patient_num
		}

		return datasetMap
	}

	/**
	 *  parse a line from PED file and form a record for DATA_BY_PATIENT_CHR column
	 *  in DE_SNP_DATA_BY_PATIENT, and this column is used for IGV and SNPViewer,
	 *  also Copy Number info goes here too.
	 *  
	 * @param line		line of PED file
	 * @return
	 */
	def getDataByPatientChr(String line){

		StringBuffer sb = new StringBuffer()
		String [] str = line.split(" +")
		for(i in 6 .. str.size()-1){
			if(i % 2 ==0) sb.append("\t" + str[i])
			else sb.append(str[i] + "\n")
		}
		return sb
	}


	void setdataByPatientAllFile(File dataByPatientAllFile){
		this.dataByPatientAllFile = dataByPatientAllFile
	}

	/*
	 File getDataByPatientAllFile(){
	 File f = new File(path + "/DataByPatient.all")
	 log.info "Create the file " + f.toString()
	 if(f.size() > 0){
	 log.info f.toString() + ":" + f.size()
	 f.delete()
	 f.createNewFile()
	 }
	 return f
	 }
	 */

	void setPedByPatientAllFile(File pedByPatientAllFile){
		this.pedByPatientAllFile = pedByPatientAllFile
	}

	/*		
	 File getPedByPatientAllFile(){
	 File f = new File(path + "/PedByPatient.all")
	 log.info "Create the file " + f.toString()
	 if(f.size() > 0){
	 log.info f.toString() + ":" + f.size()
	 f.delete()
	 f.createNewFile()
	 }
	 return f
	 }
	 */

	/**
	 * 
	 * @param deapp
	 * @return
	 */
	def setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}

	/**
	 * 
	 * @param trial
	 * @return
	 */
	def setTrial(String trial){
		this.trial = trial
	}


	def setSubjectSnpDatasetMap(Map subjectSnpDatasetMap){
		this.subjectSnpDatasetMap = subjectSnpDatasetMap
	}


	/**
	 * 
	 * @param sampleType
	 * @return
	 */
	def setSampleType(Map sampleType){
		this.sampleType = sampleType
	}

	/**
	 * 
	 * @param prefix
	 * @return
	 */
	def setPrefix(String prefix){
		this.prefix = prefix
	}


	void setMapFile(File mapFile){
		this.mapFile = mapFile
	}


	void setPedFile(File pedFile){
		this.pedFile = pedFile
	}

	void setSourceSystemPrefix(String sourceSystemPrefix){
		this.sourceSystemPrefix = sourceSystemPrefix
	}

	void setCNFile(File cnFile){
		this.cnFile = cnFile
	}

	void setPath(String path){
		this.path = path
	}

	void setPatientDimension(PatientDimension patientDimension){
		this.patientDimension = patientDimension
	}

	void setSubjectSnpDataset(SubjectSnpDataset ssd){
		this.ssd = ssd
	}

	void setCopyNumberReader(CopyNumberReader copyNumberReader){
		this.copyNumberReader = copyNumberReader
	}
}
