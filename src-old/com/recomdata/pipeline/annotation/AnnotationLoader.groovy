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

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator

import com.recomdata.pipeline.plink.SnpGeneMap
import com.recomdata.pipeline.plink.SnpInfo
import com.recomdata.pipeline.plink.SnpProbe
import com.recomdata.pipeline.transmart.GplInfo
import com.recomdata.pipeline.util.Util
import groovy.sql.Sql

class AnnotationLoader {

	private static final Logger log = Logger.getLogger(AnnotationLoader)

	static main(args) {

		PropertyConfigurator.configure("conf/log4j.properties");

		Util util = new Util()
		AnnotationLoader al = new AnnotationLoader()

		Map expectedProbes = ["GPL2005-3532.txt":59015, "GPL2004-3450.txt":57299,
					"GPL3718-44346.txt":2622264, "GPL3720-22610.txt":238304]

		Properties props = Util.loadConfiguration("conf/Annotation.properties")

		Sql deapp = Util.createSqlFromPropertyFile(props, "deapp")
		Sql biomart = Util.createSqlFromPropertyFile(props, "biomart")

		al.loadGPL(props, biomart, expectedProbes)
		println new Date()
		al.loadSnpInfo(props, deapp)
		println new Date()
		al.loadSnpProbe(props, deapp)
		println new Date()
		al.loadSnpGeneMap(props, deapp)
		println new Date()
		al.loadGplInfo(props, deapp)

		al.loadAffymetrix(props, biomart)
		al.loadTaxonomy(props, biomart)
		al.loadGeneInfo(props, biomart)

		al.loadGxGPL(props, biomart)
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
			log.info "Skip loading DE_SNP_INFO ..."
		}else{
			SnpInfo si = new SnpInfo()
			si.setDeapp(deapp)

			File snpMap = new File(props.get("destination_directory") + File.separator + props.get("snp_map_file"))
			si.loadSnpInfo(snpMap)
		}
	}


	void loadSnpProbe(Properties props, Sql deapp){

		if(props.get("skip_de_snp_probe").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_PROBE ..."
		}else{
			SnpProbe sp = new SnpProbe()
			sp.setDeapp(deapp)

			File probeInfo = new File(props.get("destination_directory") + File.separator + props.get("probe_info_file"))
			sp.loadSnpProbe(probeInfo)
		}
	}


	void loadSnpGeneMap(Properties props, Sql deapp){

		if(props.get("skip_de_snp_gene_map").toString().toLowerCase().equals("yes")){
			log.info "Skip loading DE_SNP_GENE_MAP ..."
		}else{
			SnpGeneMap sp = new SnpGeneMap()
			sp.setDeapp(deapp)

			File snpGeneMap = new File(props.get("destination_directory") + File.separator + props.get("snp_gene_map_file"))
			sp.loadSnpGeneMap(snpGeneMap)
		}
	}


	void loadGPL(Properties props, Sql biomart, Map expectedProbes){

		if(props.get("skip_gpl_annotation_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip processing GPL annotation file(s) ..."
		}else{
			GPLReader gr = new GPLReader()

			File probeInfo = new File(props.get("destination_directory") + File.separator + props.get("probe_info_file"))
			if(probeInfo.size() > 0) {
				log.warn probeInfo.toString() + " is not empty."
				probeInfo.delete()
				probeInfo.createNewFile()
			}
			gr.setProbeInfo(probeInfo)

			File snpGeneMap = new File(props.get("destination_directory") + File.separator + props.get("snp_gene_map_file"))
			if(snpGeneMap.size() > 0) {
				log.warn snpGeneMap.toString() + " is not empty."
				snpGeneMap.delete()
				snpGeneMap.createNewFile()
			}
			gr.setSnpGeneMap(snpGeneMap)


			File snpMap = new File(props.get("destination_directory") + File.separator + props.get("snp_map_file"))
			if(snpMap.size() > 0) {
				log.warn snpMap.toString() + " is not empty."
				snpMap.delete()
				snpMap.createNewFile()
			}
			gr.setSnpMap(snpMap)

			gr.setSourceDirectory(props.get("source_directory"))
			gr.setSql(biomart)
			gr.setExpectedProbes(expectedProbes)
			gr.processGPLs(props.get("input_file"))
		}
	}


	void loadAffymetrix(Properties props, Sql biomart){

		if(props.get("skip_gx_annotation_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Affymetrix GX annotation file(s) ..."
		}else{

			File annotationSource = new File(props.get("annotation_source"))

			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.HG-U133_Plus_2.txt")
			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.HG-U133A.txt")
			//File annotationSource = new File("C:/Customers/MPI/Affymetrix/Affymetrix.Celegans.txt")

			AffymetrixNetAffyGxAnnotation a = new AffymetrixNetAffyGxAnnotation()
			a.setSql(biomart)
			a.setAnnotationTable(props.get("annotation_table"))

			if(props.get("recreate_annotation_table").toString().toLowerCase().equals("yes")){
				log.info "Start recreating annotation table ${props.get("annotation_table")} for Affymetrix GX annotation file(s) ..."
				a.createAnnotationTable()
			}

			a.loadAffymetrixs(annotationSource)
		}
	}


	void loadTaxonomy(Properties props, Sql biomart){

		if(props.get("skip_taxonomy_name").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Taxonomy name ..."
		}else{
			File taxonomy = new File("C:/Customers/NCBI/Taxonomy/names.dmp")

			Taxonomy t = new Taxonomy()
			t.setBiomart(biomart)
			t.setTaxonomyTable(props.get("taxonomy_name_table"))

			t.createTaxonomyTable()
			t.loadTaxonomy(taxonomy)
		}
	}


	void loadGeneInfo(Properties props, Sql biomart){

		if(props.get("skip_gene_info").toString().toLowerCase().equals("yes")){
			log.info "Skip loading Gene Info ..."
		}else{
			File geneInfo = new File("C:/Customers/NCBI/Taxonomy/gene_info")

			GeneInfo gi = new GeneInfo()
			gi.setBiomart(biomart)
			gi.setGeneInfoTable(props.get("gene_info_table"))
			////gi.createGeneInfoTable()
			//gi.loadGeneInfo(geneInfo)
		}
	}


	void loadGxGPL(Properties props, Sql biomart){

		if(props.get("skip_gx_gpl_loader").toString().toLowerCase().equals("yes")){
			log.info "Skip loading GPL GX annotation file(s) ..."
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

}
