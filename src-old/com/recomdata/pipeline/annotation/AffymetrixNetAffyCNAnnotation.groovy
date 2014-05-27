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

import groovy.sql.Sql
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.recomdata.pipeline.util.Util

class AffymetrixNetAffyCNAnnotation {

	private static final Logger log = Logger.getLogger(AffymetrixNetAffyCNAnnotation)

	private static Properties props
	private int batchSize

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		AffymetrixNetAffyCNAnnotation cnAnno = new AffymetrixNetAffyCNAnnotation()

		if(args.size() > 0){
			log.info("Start loading property files conf/Common.properties and ${args[0]} ...")
			cnAnno.setProperties(Util.loadConfiguration(args[0]));
		} else {
			log.info("Start loading property files conf/Common.properties and conf/CN.properties ...")
			cnAnno.setProperties(Util.loadConfiguration("conf/CN.properties"));
		}

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")

		cnAnno.setBatchSize(Integer.parseInt(props.get("batch_size")))

		cnAnno.createCNProbeTable(deapp)
		cnAnno.createCNGeneTable(deapp)

		File annotationFile = new File(props.get("cn_annotation_input"))
		cnAnno.processAnnotationFile(annotationFile)

		cnAnno.loadCNProbe(deapp)
		cnAnno.loadCNProbeGene(deapp)
	}


	void loadCNProbeGene(Sql sql){

		if(props.get("skip_cn_probe_gene").toString().toLowerCase().equals("yes")){
			log.info("Skip loading data into table CN_PROBE_GENE ...")
		}else{
			log.info "Start loading data into table CN_PROBE_GENE ..."

			String qry = """ insert into cn_probe_gene (cn_id, transcript_accession, probe_gene_relationship, 
							     distance, unigene_cluster_id, gene_symbol, ncbi_gene_id, genebank_descr) 
							 values(?, ?, ?, ?, ?, ?, ?, ?) """

			int index = 0
			String [] str
			File cnGene = new File(props.get("cn_gene_output"))
			if(cnGene.size() > 0){
				//sql.withTransaction {
				sql.withBatch(batchSize, qry, { stmt ->
					cnGene.eachLine {
						index++
						if((index % 10000) == 0) {
							if((index % 100000) == 0) println index + "..."
							else print index + "..."
						}

						str = it.split("\t")

						if(str.size() > 7 && str[3].isNumber() && str[6].isNumber()){
							stmt.addBatch([
								str[0],
								str[1],
								str[2],
								str[3],
								str[4],
								str[5],
								str[6],
								str[7]
							])
						} else{
							log.info("Rejected: $it")
						}
					}
					println index
				})
				//}

				log.info "End loading data into table CN_SNP_INFO  ..."
			} else{
				log.error(cnProbe.toString() + " is empty or doesn't exist ... " )
			}
		}
	}


	void loadCNProbe(Sql sql){

		if(props.get("skip_cn_probe_info").toString().toLowerCase().equals("yes")){
			log.info("Skip loading data into table CN_PROBE_INFO  ...")
		}else{
			log.info "Start loading data into table CN_PROBE_INFO  ..."

			String qry = """ insert into cn_probe_info (cn_id, chr, chr_start, chr_stop, strand,
								 chr_x_r1, chr_x_r2, cytoband, probe_count, snp_interference,
								 gc_pct, in_final_list) values(?,?,?,?,?, ?,?,?,?,?, ?,?) """

			int index = 0
			String [] str
			File cnProbe = new File(props.get("cn_probe_output"))
			if(cnProbe.size() > 0){
				//sql.withTransaction {
				sql.withBatch(batchSize, qry, { stmt ->
					cnProbe.eachLine {
						index++
						if((index % 10000) == 0) {
							if((index % 100000) == 0) println index + "..."
							else print index + "..."
						}

						str = it.split("\t")

						if(str[2].isNumber() && str[3].isNumber() && str[8].isNumber()){
							stmt.addBatch([
								str[0],
								str[1],
								str[2],
								str[3],
								str[4],
								str[5],
								str[6],
								str[7],
								str[8],
								str[9],
								str[10],
								str[11]
							])
						} else {
							log.info ("Rejected: $it")
						}
					}
					println index
				})
				//}

				log.info "End loading data into table CN_PROBE_INFO  ..."
			} else{
				log.error(cnProbe.toString() + " is empty or doesn't exist ... " )
			}
		}
	}


	void processAnnotationFile(File annotationFile){

		String [] str
		StringBuffer sbProbe = new StringBuffer()
		StringBuffer sbGene = new StringBuffer()

		/*
		 * CSV file: Column Descriptions
		 *   1. Probe Set ID(CN-xxxxx): Unique identifier for the probe set.
		 *   2. Chromosome (1-22, X, Y): The chromosome where the CN probe set maps.
		 *   3. Chromosome Start: The start base position of the 5'-most probe in the probe set.       
		 *   4. Chromosome Stop: The end base position of the 3'-most probe in the probe set.       
		 *   5. Strand: The strand (of the reference genome) where the CN probe set maps. Missing values are represented as "---".       
		 *   6. ChrX pseudo-autosomal region 1: This pseudo-autosomal region has the following chromosomal coordinates:
		 *   			ChrX:1-2709520
		 *   			ChrY:1-2709520.
		 *        	Value is 1 if the CN probe set is located in the p-end pseudo-autosomal region of chromosome X; otherwise value is 0.
		 *   7. Cytoband: The chromosome band seen on Giemsa-stained chromosomes. Value is the band number where the CN probe set maps.
		 *   8. Associated Gene: 
		 *			Values are a list of genes which the CN probe is associated to (separated by ///). Values for each gene are transcript
		 *			accession // CN probe-gene relationship // distance (value 0 if within the gene) // UniGene Cluster ID // gene name or
		 *			symbol // NCBI Gene ID // GenBank description. The CN probe could be within the gene region or be upstream or
		 *			downstream of the genes. 
		 *	9. Microsatellite:
		 *			List of microsatellite markers surrounding or overlapping the CN probe(separated by ///).  
		 *			Values are "marker accession // CN Probe-marker relationship // distance from marker to CN Probe".  
		 *			The CN Probe-marker relationship could be "upstream" or "downstream" or "within".  The distance field is "0" for
		 *			markers that fall "within" the CN Probe.
		 *	10. Fragment Enzyme Type Length Start Stop:
		 *			Restriction fragments on which the CN Probe is located. Values are "Enzyme name // Enzyme recognition site
		 *			 // length of the restriction enzyme fragment where the CN probe set is located in // chromosomal 
		 *			 start position of the fragment // chromosomal stop position of the fragment".
		 *	11. Copy Number Variation:
		 *			Known Copy Number Variations (CNV) overlapping the corresponding CN probe set. The CNV regions were obtained 
		 *			from the Database of Genomic Variants at http://projects.tcag.ca/variations. Values are "CNV id // genomic location
		 *			// Method used to discover the CNV // Reference Pubmed id // Reference	// Variation Type".
		 * 	12. Probe Count: The values are the total number of probes in the probeset.
		 * 	13. ChrX pseudo-autosomal region 2: This pesudo autosomal region has the following chromosomal coordinates:
		 *				ChrX: 154584237-154913754
		 *				ChrY: 57443437-57772954.
		 *		    Value is 1 if the CN probe set is located in the pseudo-autosomal region of chromosome X; otherwise value is 0.
		 *	14. SNP Interference: 
		 *			Value is "YES" if there is a SNP overlapping the CN probe and  "NO" if there is no SNP overlapping the CN probe.
		 *  15. % GC: There are two types of  %GC annotations.  "local" and "A/B".  
		 *  16. OMIM: Overlaps of OMIM genes with the probeset. Values are "OMIM id // Disease title
		 *  			 // morbid map id // Transcript accession // Location of the probeset relative to the transcript
		 *
		 *	      This database contains information from the Online Mendelian Inheritance in Man (OMIM (TM)) database, 
		 *	      which has been obtained under a license from the Johns Hopkins University. This database/product does
		 *	      not represent the entire, unmodified OMIM(TM) database, which is available in its entirety at www.ncbi.nlm.nih.gov/omim/.
		 *	17. In Final List: 
		 *			Value is "YES" if the probeset is included in the final version of the library file 
		 *			and "NO" if the probeset is not included in the final version of the library file.
		 */

		if(props.get("skip_process_annotation_file").toString().toLowerCase().equals("yes")){
			log.info("Skip processing: ${annotationFile.toString()} ...")
		}else{
			log.info "Start processing: ${annotationFile.toString()} ..."

			if(annotationFile.size() >0){
				annotationFile.eachLine {
					str = it.split(props.get("column_seperator"))
					//if((it.indexOf('"CN_') >=0) && (str[2].indexOf("---") == -1)){
					if(it.indexOf('"CN_') >=0){
						sbProbe.append(str[0].replace('"', '').trim() + "\t")
						sbProbe.append(str[1].trim() + "\t")
						sbProbe.append(str[2].trim() + "\t")
						sbProbe.append(str[3].trim() + "\t")
						sbProbe.append(str[4].trim() + "\t")
						sbProbe.append(str[5].trim() + "\t")
						sbProbe.append(str[6].trim() + "\t")

						// 8. Associated Gene
						sbGene.append(getGene(str[0].replace('"', '').trim(), str[7]))

						// 12. Probe Count
						sbProbe.append(str[11].trim() + "\t")

						//  13. ChrX pseudo-autosomal region 2
						sbProbe.append(str[12].trim() + "\t")

						//  14. SNP Interference
						sbProbe.append(str[13].trim() + "\t")

						// 15. % GC
						sbProbe.append(str[14].trim() + "\t")

						// 17. In  Final List
						sbProbe.append(str[16].replace('"', '').trim() + "\n")
					}
				}

				File probe = new File(props.get("cn_probe_output"))
				if(probe.size() > 0){
					probe.delete()
					probe.createNewFile()
				}
				probe.append(sbProbe.toString())
				sbProbe.setLength(0)

				File gene = new File(props.get("cn_gene_output"))
				if(gene.size() > 0){
					gene.delete()
					gene.createNewFile()
				}
				gene.append(sbGene.toString())
				sbGene.setLength(0)
			}else{
				log.error(annotationFile.toString() + " is empty or not exit ...")
			}
		}
	}


	StringBuffer getGene(String probeId, String associatedGene){

		StringBuffer sbGene = new StringBuffer()

		String [] str, tmp

		if(associatedGene.indexOf("///") >= 0){
			str = associatedGene.split("///")
			str.each{
				if(it.indexOf("//") >= 0) {
					tmp = it.split("//")
					sbGene.append(probeId + "\t")

					tmp.each{
						sbGene.append(it.trim() + "\t")
					}
					sbGene.append("\n")
				}
			}
		} else{
			//sbGene.append(probeId + "\t" + associatedGene.replace(" // ", "\t") + "\n")
			if(associatedGene.indexOf("//")  >= 0){
				tmp = associatedGene.split("//")
				sbGene.append(probeId + "\t")

				tmp.each{
					sbGene.append(it.trim() + "\t")
				}
				sbGene.append("\n")
			}
		}
		return sbGene
	}


	void createCNProbeTable(Sql sql){

		String cnProbeTable = props.get("cn_probe_table")

		if(props.get("skip_create_cn_probe_table").toString().toLowerCase().equals("yes")){
			log.info("Skip creating table: ${cnProbeTable} ...")
		}else{
			log.info "Start creating table: ${cnProbeTable}"

			String qry = """ create table ${cnProbeTable} (
								cn_id			varchar2(20),
								chr				varchar2(5),
								chr_start		number(10),
								chr_stop		number(10),
								strand			varchar2(4),
								chr_x_r1		varchar2(20),
								chr_x_r2		varchar2(20),
								cytoband		varchar2(10),
								probe_count		number(4),
								snp_interference		varchar2(10),
								gc_pct		varchar2(10),
								in_final_list	varchar2(10)
							 ) nologging
						 """

			String qry1 = "select count(*)  from user_tables where table_name=?"
			if(sql.firstRow(qry1, [cnProbeTable.toUpperCase()])[0] > 0){
				qry1 = "drop table ${cnProbeTable} purge"
				sql.execute(qry1)
			}

			sql.execute(qry)

			log.info "End creating table: ${cnProbeTable}"
		}
	}


	void createCNGeneTable(Sql sql){

		String cnGeneTable = props.get("cn_gene_table")

		if(props.get("skip_create_cn_gene_table").toString().toLowerCase().equals("yes")){
			log.info("Skip creating table: ${cnGeneTable} ...")
		}else{
			log.info "Start creating table: ${cnGeneTable}"

			String qry = """ create table ${cnGeneTable} (
								cn_id			varchar2(20),
								transcript_accession		varchar2(100),
								probe_gene_relationship		varchar2(200),
								distance			number(10),
								unigene_cluster_id	varchar2(20),
								gene_symbol			varchar2(200),
								ncbi_gene_id		number(10),
								genebank_descr		varchar2(1000)
							 ) nologging
						 """

			String qry1 = "select count(*)  from user_tables where table_name=?"
			if(sql.firstRow(qry1, [cnGeneTable.toUpperCase()])[0] > 0){
				qry1 = "drop table ${cnGeneTable} purge"
				sql.execute(qry1)
			}

			sql.execute(qry)

			log.info "End creating table: ${cnGeneTable}"
		}
	}


	void setBatchSize(int batchSize){
		this.batchSize = batchSize
	}


	void setProperties(Properties props){
		this.props = props
	}
}
