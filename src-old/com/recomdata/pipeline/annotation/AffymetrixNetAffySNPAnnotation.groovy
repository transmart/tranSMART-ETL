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

import com.recomdata.pipeline.plink.SnpGeneMap
import com.recomdata.pipeline.plink.SnpInfo
import com.recomdata.pipeline.plink.SnpProbe
import com.recomdata.pipeline.transmart.GplInfo
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator


class AffymetrixNetAffySNPAnnotation {

	private static final Logger log = Logger.getLogger(AffymetrixNetAffySNPAnnotation)

	Sql biomart, deapp
	String annotationTable, snpGeneTable, fieldSeperator, annotationFilePattern

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()
		AffymetrixNetAffySNPAnnotation netAffy = new AffymetrixNetAffySNPAnnotation()

		Properties props = Util.loadConfiguration("conf/loader.properties")

		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")
		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		netAffy.loadAffymetrix(props, deapp)
		netAffy.loadGplInfo(props, deapp)
		netAffy.loadSnpInfo(props, deapp)
		netAffy.loadSnpProbe(props, deapp)
		netAffy.loadSnpGeneMap(props, deapp)
	}


	void loadGplInfo(Properties props, Sql deapp){

		if(props.get("skip_de_gpl_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_GPL_INFO ..."
		}else{
			GplInfo gi = new GplInfo()
			gi.setDeapp(deapp)

			Map gplMap = [:]
			gplMap["platform"] = props.get("platform")
			gplMap["title"] = props.get("title")
			gplMap["organism"] = props.get("organism")
			gplMap["markerType"] = props.get("marker_type")
			gi.insertGplInfo(gplMap)
		}
	}


	void loadSnpInfo(Properties props, Sql deapp){

		if(props.get("skip_de_snp_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_INFO table ..."
		}else{
			log.info "Start loading into DE_SNP_INFO table from ${props.get("affymetrix_annotation_table")} table ..."

			SnpInfo snpInfo = new SnpInfo()
			snpInfo.setAnnotationTable(props.get("affymetrix_annotation_table"))
			snpInfo.setDeapp(deapp)

			Map columnMap = [:]
			columnMap["name"] = "probe_id"
			columnMap["chr"] = "chr"
			columnMap["pos"] = "position"
			snpInfo.loadSnpInfo(columnMap)
		}
	}


	void loadSnpProbe(Properties props, Sql deapp){

		if(props.get("skip_de_snp_probe").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_PROBE table ..."
		}else{
			log.info "Start loading into DE_SNP_PROBE table from ${props.get("affymetrix_annotation_table")} table ..."

			SnpProbe snpProbe = new SnpProbe()
			snpProbe.setAnnotationTable(props.get("affymetrix_annotation_table"))
			snpProbe.setDeapp(deapp)

			Map columnMap = [:]
			columnMap["probe"] = "probe_id"
			columnMap["rs"] = "rs_id"
			snpProbe.loadSnpProbe(columnMap)
		}
	}


	void loadSnpGeneMap(Properties props, Sql deapp){

		if(props.get("skip_de_snp_gene_map").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_GENE_MAP table ..."
		}else{
			log.info "Start loading into DE_SNP_GENE_MAP table from ${props.get("affymetrix_annotation_table")} table ..."

			SnpGeneMap snpGeneMap = new SnpGeneMap()
			snpGeneMap.setAnnotationTable(props.get("affymetrix_annotation_table"))
			snpGeneMap.setDeapp(deapp)

			Map columnMap = [:]
			columnMap["probe"] = "probe_id"
			columnMap["gene_id"] = "gene_id"
			snpGeneMap.loadSnpGeneMap(columnMap)
		}
	}


	void loadAffymetrix(Properties props, Sql biomart){

		if(props.get("skip_affymetrix_annotation_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Affymetrix NetAffy CSV annotation file(s) ..."
		}else{

			log.info "Start loading Affymetrix NetAffy CSV annotation file(s) from ${props.get("affymetrix_annotation_source")} ..."

			File annotationSource = new File(props.get("affymetrix_annotation_source"))

			setSql(biomart)
			setAnnotationTable(props.get("affymetrix_annotation_table"))
			//setSnpGeneTable(props.get("affymetrix_snp_gene_table"))
			setAnnotationFilePattern(props.get("annotationFilePattern"))
			setFieldSeperator(props.get("field_seperator"))

			if(props.get("recreate_affymetrix_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating Affymetrix annotation table ${props.get("affymetrix_annotation_table")} ..."
				createAnnotationTable()

				//log.info "Start recreating Affymetrix SNP-Gene mapping table ${props.get("affymetrix_snp_gene_table")} ..."
				//createSNPGeneTable()
			}

			loadAffymetrixs(annotationSource)
		}
	}



	/**
	 * Process Affymetrix NetAffy annotation files from Affymetrix
	 *
	 * @param input		an Affymetrix NetAffy annotation file or a directory with Affymetrix annotation files
	 */
	void loadAffymetrixs(File input){

		if(input.isDirectory()){
			input.eachFile {
				if((it.toString().indexOf(annotationFilePattern) != -1)){
					log.info it
					loadAffymetrixAll(it)
				}
			}
		} else{
			if(input.isFile()) {
				loadAffymetrixAll(input)
			} else {
				log.info "Neither a directory nor a file: " + input.toString()
			}
		}
	}


	/*
	 * Process Affymetrix annotation file
	 *
	 */
	void loadAffymetrix(File input){

		String platform = input.toString().split(/\./)[0]
		log.info "Input File: " + input.toString() + "\t Platform: " + platform

		if(isAnnotationExist(platform)) {
			log.warn "Annotaion data for ${input.toString()} already exist."
			return
		}

		// store records need to be loaded
		File output = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".tsv")
		log.info "Output file: " + output.toString()
		if(output.size() > 0){
			output.delete()
		}

		// store records need to be manually checked
		File reject = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".reject")
		log.info "File for rejected records: " + reject.toString()
		if(reject.size() > 0){
			reject.delete()
		}


		// store discarded records, either control probes or w/o gene association
		File discard = new File(input.getParent() + "/" + input.toString().split(/\./)[1] + ".discard")
		log.info "File for rejected records: " + discard.toString()
		if(discard.size() > 0){
			discard.delete()
		}

		// pick up needed columns from annotation file
		StringBuffer sb = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()
		StringBuffer sbDiscard = new StringBuffer()

		boolean isContinue = false
		if(input.size() > 0){
			int line = 0
			input.eachLine {
				line++
				String [] str = it.split("\t")
				if(line==1){ // it.indexOf("Probe Set ID") !=-1){
					isContinue = isCorrectFormat(it)
					for(int i in 0..str.size()-1) println i + "\t" + str[i]

				}

				if(isContinue && line > 1){
					if((str.size() > 15) && (str[0].indexOf("AFFX-") == -1) && (str[1].indexOf("---") == -1)){
						if(str[1].indexOf("///") != -1){
							//println  str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[3] + "\t" + str[15]
							String [] gene = str[1].split("///")
							String [] symbol =  str[2].split("///")
							String [] geneId =  str[15].split("///")

							if((gene.size() != symbol.size()) || (gene.size() != geneId.size())){
								sbReject.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[3] + "\t" + str[15] + "\n")
							} else {
								for(int k in 0 .. gene.size()-1){
									sb.append(platform + "\t" + str[3] + "\t" + str[0] + "\t" + gene[k].trim() + "\t" + symbol[k].trim() + "\t" + geneId[k].trim() + "\n")
								}
							}
						}else{
							if((str[2].indexOf("///") != -1) || (str[15].indexOf("///") != -1)){
								sbReject.append(it + "\n")
							} else{
								sb.append(platform + "\t" + str[3] + "\t" + str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[15] + "\n")
							}
						}
					}else{
						sbDiscard.append(it + "\n")
					}
				}
			}
		}else{
			log.error("Empty file: " + input.toString())
		}

		// write data from StringBuffer to files
		if(sb.size() > 0) output.append(sb.toString())
		if(sbReject.size() > 0) reject.append(sbReject.toString())
		if(sbDiscard.size() > 0) discard.append(sbDiscard.toString())

		// load probe records with gene symbol and gene id into database
		insertAffymetrix(output)
	}


	void loadAffymetrixAll(File input){

		String species = ""
		String platform = ""

		// for Windows enviornment
		if(input.toString().indexOf("\\") != -1) platform = input.toString().split(/\./)[0].split ("\\\\")[-1]
		// for *nix environments
		else platform = input.toString().split(/\./)[0].split ("/")[-1]
		log.info "Input File: " + input.toString() + "\n\t Platform: " + platform

		//if(isAnnotationExist(platform)) {
		//	log.warn "Annotaion data for ${input.toString()} already exist."
		//	return
		//}

		// create a map from an annotation file
		File plinkMap = new File(input.getParent() + "/" + platform + ".map")
		log.info "PLINK map file: " + plinkMap.toString()
		if(plinkMap.size() > 0){
			plinkMap.delete()
		}

		// store records need to be loaded
		File output = new File(input.getParent() + "/" + platform + ".tsv")
		log.info "Output file: " + output.toString()
		if(output.size() > 0){
			output.delete()
		}

		// store records need to be manually checked
		File reject = new File(input.getParent() + "/" + platform + ".reject")
		log.info "File for rejected records: " + reject.toString()
		if(reject.size() > 0){
			reject.delete()
		}

		// pick up needed columns from annotation file
		StringBuffer sbResult = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()

		boolean isContinue = false, isFirstLine = true
		Map record = [:]
		int line = 0
		int rejectedLine = 0
		if(input.size() > 0){
			input.eachLine {

				if(it.indexOf("#")!= 0){
					if(it.indexOf("Probe Set ID") != -1){
						//print "$line \t $it \n"
						String [] header = it.split(",")
						for(int i in 0..header.size()-1) print "$i   ${header[i]} \n"
						if(!isCorrectFormat(it, fieldSeperator)) {
							throw new Exception("Please check the format of file: ${input.toString()}")
						}
					}else{
						if(it.indexOf("SNP_A-") != -1){
							line++
							record = readNetAffymetrixSNPLine(it, fieldSeperator)
							if(record["result"].size() > 0) sbResult.append(record["result"].toString())
							else {
								rejectedLine++
								println "$rejectedLine: $it"
							}
						}else{
							sbReject.append(it + "\n")
						}
					}
				}
			}
		}else{
			log.error("Empty file: " + input.toString())
		}

		println "Total SNPs: $line \n"

		// write data from StringBuffer to files
		if(sbResult.size() > 0) {
			Map rs = [:]
			sbResult.each{ rs[it] = 1 }

			StringBuffer sbMap = new StringBuffer()
			StringBuffer sb = new StringBuffer()
			rs.each{ k, v ->
				sb.append(k + "\n")
			}
			output.append(sb.toString())
		}

		//if(sbReject.size() > 0) reject.append(sbReject.toString())
		//if(sbDiscard.size() > 0) discard.append(sbDiscard.toString())

		// load probe records with gene symbol and gene id into database
		insertAffymetrix(output)
		createPlinkMap(plinkMap, output)
	}


	void createPlinkMap(File plinkMap, File input){

		Map mapRecord = [:]
		StringBuffer sb = new StringBuffer()
		if(input.size() >0){
			input.eachLine {
				String [] line = it.split("\t")
				if(line.size() > 3) mapRecord["${line[2]}\t${line[0]}\t0\t${line[3]}\n"] = 1 // sb.append("${line[2]}\t${line[0]}\t0\t${line[3]}\n")
				else println it
			}
		} else{
			log.error("Empty file: " + input.toString())
		}

		mapRecord.each{ k, v ->
			sb.append(k)
		}
		plinkMap.append(sb.toString())
	}


	Map readNetAffymetrixSNPLine(String line, String fieldSeperator){

		String [] str = line.split(fieldSeperator)
		Map record = [:]
		StringBuffer sbResult = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()
		StringBuffer sbDiscard = new StringBuffer()

		if(str.size() > 10){
			String snp = str[0].replace("\"", "")
			String rs = str[1].replace("\"", "")
			String chr = str[2].replace("\"", "")
			String pos = str[3].replace("\"", "")
			String strand = str[4].replace("\"", "")

			String Cytoband = str[6].replace("\"", "")
			String allele_A = str[8].replace("\"", "")
			String allele_B = str[9].replace("\"", "")
			String associatedGene = str[10].replace("\"", "")

			Map genes = readGeneAssignments(associatedGene)

			//print "$line \n "
			//print "$associatedGene \n"
			//Util.printMap(genes)

			genes.each{ k, v ->
				String [] s = k.split("\t")
				sbResult.append(snp + "\t" + rs + "\t" + chr + "\t" + pos + "\t" + s[0] + "\t" + s[1] + "\t" + s[2] + "\n")
			}
		}

		record["result"] = sbResult

		return record
	}


	Map readAffymetrixLine(String platform, String line){

		String [] str = line.split("\t")
		Map record = [:]
		StringBuffer sbResult = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()
		StringBuffer sbDiscard = new StringBuffer()

		if((str.size() > 15) && (str[0].indexOf("AFFX-") == -1) && (str[1].indexOf("---") == -1)){
			if(str[1].indexOf("///") != -1){
				String [] gene = str[1].split("///")
				String [] symbol =  str[2].split("///")
				String [] geneId =  str[15].split("///")

				if((gene.size() != symbol.size()) || (gene.size() != geneId.size())){
					sbReject.append(str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[3] + "\t" + str[15] + "\n")
				} else {
					for(int k in 0 .. gene.size()-1){
						sbResult.append(platform + "\t" + str[3] + "\t" + str[0] + "\t" + gene[k].trim() + "\t" + symbol[k].trim() + "\t" + geneId[k].trim() + "\n")
					}
				}
			}else{
				if((str[2].indexOf("///") != -1) || (str[15].indexOf("///") != -1)){
					sbReject.append(line + "\n")
				} else{
					sbResult.append(platform + "\t" + str[3] + "\t" + str[0] + "\t" + str[1] + "\t" + str[2] + "\t" + str[15] + "\n")
				}
			}
		}else{
			sbDiscard.append(line + "\n")
		}

		record["result"] = sbResult
		record["reject"] = sbReject
		record["discard"] = sbDiscard

		return record
	}



	Map readAffymetrixSTLine(String platform, String species, String line){

		String [] str = line.split("\t")

		Map record = [:]
		StringBuffer sbResult = new StringBuffer()
		StringBuffer sbReject = new StringBuffer()
		StringBuffer sbDiscard = new StringBuffer()

		if((str.size() > 7) && (!str[8].equals(null))){
			if(str[8].indexOf("///")) {
				Map probeGene = readGeneAssignments(str[8])
				probeGene.each {k, v ->
					sbResult.append(platform + "\t" + species + "\t" + str[0] + "\t" + k + "\n")
				}
			} else {
				if(readGeneAssignment(str[8]).size() >0){
					sbResult.append(platform + "\t" + species + "\t" + str[0] + "\t" + readGeneAssignment(str[8]) + "\n")
				}else{
					sbReject.append(str[0] + "\t" + str[8] + "\n")
				}
			}
		}else{
			sbDiscard.append(line)
		}

		record["result"] = sbResult
		record["reject"] = sbReject
		record["discard"] = sbDiscard

		return record
	}



	Map readGeneAssignments(String geneAssignments){

		Map probeGene = [:]
		String [] str = geneAssignments.split("///")
		str.each{
			if(it.indexOf("//") != -1) {
				String record = readGeneAssignment(it)
				//println record
				if(record.size() > 0) probeGene[record] = 1
			}
		}
		return probeGene
	}


	String readGeneAssignment(String geneAssignment){

		String [] str = geneAssignment.split("//")
		if(!str[1].equals(null) &&  str[1].trim().size() > 0){
			return str[5].trim() + "\t" + str[4].trim() + "\t" + str[6].trim()
		} else{
			return ""
		}
	}


	/**
	 *  Load processed Affymetrix annotation data into database
	 *
	 * @param input
	 */
	void insertAffymetrix(File input){
		String [] str
		if(input.size() >0){
			biomart.withTransaction {
				biomart.withBatch("insert into $annotationTable (probe_id,rs_id,chr,position,gene_id,gene_symbol,gene_descr) values (?,?,?,?,?,?,?)",
						{ ps ->
							input.eachLine{
								if(it.trim().size() > 0) {
									str = it.split(/\t/)
									ps.addBatch([
										str[0],
										str[1],
										str[2],
										str[3],
										str[4],
										str[5],
										str[6]
									])
								}
							}
						})
			}
		}else{
			log.error("Empty file: " + input.toString())
		}
	}


	boolean isAnnotationExist(String platform){
		String qry = "select count(*) from $annotationTable where upper(platform)=?"
		if(biomart.firstRow(qry, [platform.toUpperCase()])[0] > 0){
			return true
		} else {
			return false
		}
	}


	/**
	 *  The format for Affymetrix GX Annotation file should be as the following:
	 *
	 0	Probe Set ID
	 1	Gene Symbol
	 2	Gene Title
	 3	Species Scientific Name
	 4	Sequence Type
	 5	Sequence Source
	 6	Transcript ID[Array Design]
	 7	Target Description
	 8	Representative Public ID
	 9	Archival UniGene Cluster
	 10	UniGene ID
	 11	Alignments
	 12	Chromosomal Location
	 13	Unigene Cluster Type
	 14	Ensembl
	 15	Entrez Gene
	 16	SwissProt
	 17	EC
	 18	OMIM
	 19	RefSeq Protein ID
	 20	RefSeq Transcript ID
	 21	FlyBase
	 22	AGI
	 23	WormBase
	 24	MGI Name
	 25	RGD Name
	 26	SGD accession number
	 27	Gene Ontology Biological Process
	 28	Gene Ontology Cellular Component
	 29	Gene Ontology Molecular Function
	 30	Pathway
	 31	InterPro
	 32	Trans Membrane
	 33	QTL
	 34	Annotation Transcript Cluster
	 35	Transcript Assignments
	 * @param header
	 * @return
	 */
	boolean isCorrectFormat(String header, String fieldSeperator){

		String [] str = header.split(fieldSeperator)

		if(!str[0].toString().replace("\"", "").toUpperCase().equals("Probe Set ID".toUpperCase())) {
			log.error("Actual header didn't match the expected one: ${str[0]} vs 'Probe Set ID'.")
			return false
		} else if(!str[1].toString().toUpperCase().equals("dbSNP RS ID".toUpperCase())) {
			log.error("Actual header didn't match the expected one: ${str[1]} vs 'dbSNP RS ID'.")
			return  false
		} else if(!str[2].toString().toUpperCase().equals("Chromosome".toUpperCase())) {
			log.error("Actual header didn't match the expected one: ${str[2]} vs 'Chromosome'.")
			return false
		} else if(!str[3].toString().toUpperCase().equals("Physical Position".toUpperCase())) {
			log.error("Actual header didn't match the expected one: ${str[3]} vs 'Physical Position'.")
			return false
		} else if(!str[10].toString().toUpperCase().equals("Associated Gene".toUpperCase())) {
			log.error("Actual header didn't match the expected one: ${str[10]} vs 'Associated Gene'.")
			return false
		} else {
			return true
		}
	}

	/**
	 *
	 * @param header	HuGene-1_0-st-v1, MoGene-1_0-st-v1 and RaGene-1_0-st-v1
	 *
	 0	transcript_cluster_id
	 1	Gene Symbol
	 2	probeset_id
	 3	seqname
	 4	strand
	 5	start
	 6	stop
	 7	total_probes
	 8	gene_assignment
	 9	mrna_assignment
	 10	swissprot
	 11	unigene
	 12	GO_biological_process
	 13	GO_cellular_component
	 14	GO_molecular_function
	 15	pathway
	 16	protein_domains
	 17	crosshyb_type
	 18	category
	 * @return
	 */
	boolean isCorrectSTFormat(String header){

		String [] str = header.split("\t")

		if(!str[0].toString().toLowerCase().equals("transcript_cluster_id")) {
			log.error("Column 1 didn't match the expected: ${str[0]} vs 'transcript_cluster_id'.")
			return false
		} else if(!str[1].toString().toLowerCase().equals("gene symbol")) {
			log.error("Column 2 didn't match the expected: ${str[1]} vs 'Gene Symbol'.")
			return  false
		} else if(!str[3].toString().toLowerCase().equals("seqname")) {
			log.error("Column 3 didn't match the expected: ${str[3]} vs 'seqname'.")
			return false
		} else if(!str[8].toString().toLowerCase().equals("gene_assignment")) {
			log.error("Column 8 didn't match the expected: ${str[8]} vs 'gene_assignment'.")
			return false
		} else {
			return true
		}
	}



	void createAnnotationTable(){
		String qry = "select count(*) from user_tables where table_name=?"
		if(biomart.firstRow(qry, [
			annotationTable.toUpperCase()
		])[0] > 0){
			log.info "The existing table $annotationTable will be rename to ${annotationTable}_bk"
			qry = "drop table $annotationTable purge"
			biomart.execute(qry)
		}

		log.info "Create table $annotationTable ..."
		qry = """ create table $annotationTable(
					probe_id 	varchar2(100),
					rs_id 	    varchar2(100),
					chr     	varchar2(2),
					position 	number(10),
					gene_symbol varchar2(100),
					gene_descr  varchar2(1000),
					gene_id    	varchar2(20)
				)"""
		biomart.execute(qry)
	}


	void createSNPGeneTable(){
		String qry = "select count(*) from user_tables where table_name=?"
		if(biomart.firstRow(qry, [
			snpGeneTable.toUpperCase()
		])[0] > 0){
			log.info "The existing table $snpGeneTable will be rename to ${snpGeneTable}_bk"
			qry = "drop table $snpGeneTable purge"
			biomart.execute(qry)
		}

		log.info "Create table $snpGeneTable ..."
		qry = """ create table $snpGeneTable(
					platform 	varchar2(100),
					probe_id 	varchar2(100),
					rs_id 	    varchar2(100),
					chr     	varchar2(2),
					position 	number(10),
					gene_symbol varchar2(100),
					gene_descr  varchar2(1000),
					gene_id    	varchar2(20)
				)"""
		biomart.execute(qry)
	}


	void setAnnotationTable(String annotationTable){
		this.annotationTable = annotationTable
	}


	void setSnpGeneTable(String snpGeneTable){
		this.snpGeneTable = snpGeneTable
	}


	void setAnnotationFilePattern(String annotationFilePattern){
		this.annotationFilePattern = annotationFilePattern
	}


	void setFieldSeperator(String fieldSeperator){
		this.fieldSeperator = fieldSeperator
	}

	void setSql(Sql biomart){
		this.biomart = biomart
	}
}