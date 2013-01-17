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

import groovy.sql.Sql

class SnpProbeSortedDef {

	private static final Logger log = Logger.getLogger(SnpProbeSortedDef)

	Sql deapp
	String platform, mapDirectory

	/**
	 *   extract data from PLINK's MAP file and format data for
	 *      DE_SNP_PROBE_SORTED_DEF table's PROBE_DEF column
	 *      and later used by IGV and GWAS
	 *
	 * @param chr	chromosome
	 * @return
	 */
	Map getSnpProbeDefByChr(String chr){

		int totalProbes = 0;
		StringBuffer probe_def = new StringBuffer()
		File map

		if(chr.toLowerCase().indexOf("all") >= 0){
			map = new File(mapDirectory + "/all.map")
		}else{
			map = new File(mapDirectory + "/chr" + chr + ".map")
		}

		if(map.exists() && map.size() > 0){
			log.info "Create DE_SNP_PROBE_SORTED_DEF record for Chromosome: " + chr

			map.eachLine{
				totalProbes++
				String [] str = it.split("\t")

				// snp_id, chromosome, position
				probe_def.append(str[1] + "\t" + str[0] + "\t" + str[3] + "\n")
			}
		}else{
			log.error "The map file for Chromosome " + chr + ": " + map.toString() + " doesn't exist or is empty ... "
		}

		log.info "Total probes for Chromosome " + chr + ": " + totalProbes

		return ['total':totalProbes, 'snpDef':probe_def]
	}


	/**
	 *   check if definition for a particular platform's chromosome exists, 
	 *   if not, a new record will be added into DE_SNP_PROBE_SORTED_DEF
	 *    
	 * @param sql			database handler, should point to DEAPP schema
	 * @param platform		platform used in the SNP array
	 * @param chr			chromosome ( 1 - 26)
	 * @param n				total probes for a chromosome
	 * @param snpDef		SNP definition in a particular format
	 */
	void loadSnpDefByChr(String chr, int n, String snpDef){

		String qry = """ insert into de_snp_probe_sorted_def
				           (platform_name, num_probe, chrom, probe_def, snp_id_def)
				         values(?, ?, ?, ?, ?) """

		if(isSnpProbetSortedDefExist(platform, chr)){
			log.info "DE_SNP_PROBE_SORTED_DEF already have a record for $platform's chromosome $chr ... "
		} else {
			log.info "Start inserting DE_SNP_PROBE_SORTED_DEF for $platform's chromosome $chr ... "
			deapp.execute qry, [
				platform,
				n,
				chr.toUpperCase(),
				snpDef,
				snpDef
			]
			log.info "End inserting DE_SNP_PROBE_SORTED_DEF for $platform's chromosome $chr ... "
		}
	}


	void loadSnpDefByChromosomes(List chrs){

		Map map = [:]
		// ignore 25 (XY) and 26 (MT) for now
		chrs.each{chr ->
			map = getSnpProbeDefByChr(chr)
			loadSnpDefByChr(chr, map['total'], map['snpDef'].toString())
		}
	}

	/**
	 *   extract ALL chromosome's data from PLINK's MAP file, format SNP def
	 *      for DE_SNP_PROBE_SORTED_DEF table's PROBE_DEF column 
	 *      and later used by IGV and GWAS
	 *
	 * @return
	 */

	def getSnpRsDefByAll(){

		log.info "Start creating SNP def for ALL chromosomes ..."

		StringBuffer snpDef = new StringBuffer()
		def total = 0

		// ignore 25(XY) and 26(MT) for now
		for (chr in 1 .. 24){
			def map = getSnpRsDefByChr(chr.toString())
			total += map['total']
			snpDef.append(map['snpDef'].toString())
		}

		log.info "End creating SNP def for ALL chromosomes ..."

		return ["total":total, "snpDef":snpDef]
	}


	/**
	 *  check if there is a record for (platform, chromsome) in DE_SNP_PROBE_SORTED_DEF
	 *   
	 * @param platform
	 * @param chr
	 * @return
	 */
	boolean isSnpProbetSortedDefExist(String platform, String chr){
		String qry = "select count(1) from de_snp_probe_sorted_def where platform_name=? and chrom = ? "
		def obj = deapp.firstRow(qry, [platform, chr])
		if(obj[0] > 0) return true
		else return false
	}


	def setSqlForDeapp(Sql deapp){
		this.deapp = deapp
	}


	def setPlatform(String platform){
		this.platform = platform
	}


	void setMapDirectory(String mapDirectory){
		this.mapDirectory = mapDirectory
	}
}
