/*************************************************************************
 * tranSMART - translational medicine data mart
 *
 * Copyright 2014 NKI/AVL
 *
 * This product includes software developed at NKI/AVL
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
package org.transmartproject.pipeline.tworegion

import org.transmartproject.pipeline.util.HighDimImport
import groovy.sql.BatchingPreparedStatementWrapper

import java.sql.BatchUpdateException

/**
 * Created by j.hudecek on 7-8-2014.
 */
class TwoRegion extends HighDimImport {
    private static HashMap<String, Integer> genes;
    private static String queryCombined =
               "INSERT INTO deapp.de_two_region_junction\
                         (up_chr, up_pos, up_strand, up_end, down_chr, down_pos, down_strand, down_end, is_in_frame, external_id, assay_id) \
                 VALUES (:up_chr,:up_pos,:up_strand,:up_end,:down_chr,:down_pos,:down_strand,:down_end, :is_in_frame,:external_id,:assay_id);\
                INSERT INTO deapp.de_two_region_event \
                         (cga_type, soap_class) \
                 VALUES (:cga_type, :soap_type);\
                INSERT INTO deapp.de_two_region_junction_event \
                        (junction_id, event_id,  \
                        reads_span, reads_junction, pairs_span, pairs_junction,  \
                        pairs_end, reads_counter, base_freq) \
                 VALUES (currval( 'de_two_region_junction_seq'), currval( 'de_two_region_event_seq'),\
                       :reads_span, :reads_junction, :pairs_span, :pairs_junction,\
                       :pairs_end, :reads_counter, :base_freq );\
                INSERT INTO deapp.de_two_region_event_gene( \
                        gene_id, event_id, effect) VALUES   \
                      (:up_gene, currval( 'de_two_region_event_seq'), 'FUSION');\
                INSERT INTO deapp.de_two_region_event_gene( \
                        gene_id, event_id, effect) VALUES   \
                      (:down_gene, currval( 'de_two_region_event_seq'), 'FUSION');";
    private static String queryCombinedOracle =
            "BEGIN INSERT INTO deapp.de_two_region_junction\
                      (up_chr, up_pos, up_strand, up_end, down_chr, down_pos, down_strand, down_end, is_in_frame, external_id, assay_id) \
              VALUES (:up_chr,:up_pos,:up_strand,:up_end,:down_chr,:down_pos,:down_strand,:down_end, :is_in_frame,:external_id,:assay_id);\
             INSERT INTO deapp.de_two_region_event \
                      (cga_type, soap_class) \
              VALUES (:cga_type, :soap_type);\
             INSERT INTO deapp.de_two_region_junction_event \
                     (junction_id, event_id,  \
                     reads_span, reads_junction, pairs_span, pairs_junction,  \
                     pairs_end, reads_counter, base_freq) \
              VALUES (de_two_region_junction_seq.currval, de_two_region_event_seq.currval,\
                    :reads_span, :reads_junction, :pairs_span, :pairs_junction,\
                    :pairs_end, :reads_counter, :base_freq );\
             INSERT INTO deapp.de_two_region_event_gene( \
                     gene_id, event_id, effect) VALUES   \
                   (:up_gene, de_two_region_event_seq.currval, 'FUSION');\
             INSERT INTO deapp.de_two_region_event_gene( \
                     gene_id, event_id, effect) VALUES   \
                   (:down_gene, de_two_region_event_seq.currval, 'FUSION'); END;";
    static main(args) {
        procedureName = "load_tworegion";
        initDB()

        //check args, print usage
        if (!parseOptions(args)) {
            return
        };
        if (isOracle) {
            queryCombined = queryCombinedOracle;
        }


        cleanupOptions()
        startAudit()
        try {
            insertConceptPath()

            //switch cgs, tophat, soapfuse
            if (options.cgaJunctions != false) {
                sampleId = options.sampleId;
                subjectId = options.subjectId;
                initSourceSystem(options.sampleId,  options.subjectId)

                insertMetadata()

                insertCgaJunctions()
                stepCt++;

                insertCgaEvents()
            } else if (options.tophatFusionPost != false) {
                readMappingFile(options.mapping)

                importTophatJunctions()
            } else if (options.soapFusion != false) {
                readMappingFile(options.mapping)
                importSoapJunctions()
            } else {
                throw new IllegalArgumentException("Nothing to do, soap, tophat or cga needs to be specified")
            }
            endAudit('SUCCESS');
        }
        catch (Exception ex) {
            handleError(ex)
        }
    }

    private static void importSoapJunctions() {
        //withBatch on the post fusion file, each line specifies one junction which creates one fusion
        def totalUpdated = 0;
        try {
            new File(options.soapFusion).eachLine {line ->
                if (line.startsWith('sample\t')) {
                    return
                }; //header line
                def tokens = line.split();
                //sample	up_gene	dw_gene	up_chr	up_strand	up_Genome_pos	up_loc	dw_chr	dw_strand	dw_Genome_pos	dw_loc	Span_reads_num	Junc_reads_num	Fusion_Type	down_fusion_part_frame.shift_or_not	fusionscore	fusionscore2	fusionscore3	sum_junc_and_span
                deapp.execute(queryCombined, [
                        'up_gene'    : tokens[1],
                        'down_gene'  : tokens[2],
                        'up_chr'     : tokens[3].substring(3),
                        'up_pos'     : Integer.parseInt(tokens[5]),
                        'up_strand'  : tokens[4],
                        'up_end'     : Integer.parseInt(tokens[5]),
                        'down_chr'   : tokens[7].substring(3),
                        'down_pos'   : Integer.parseInt(tokens[9]),
                        'down_strand': tokens[8],
                        'down_end'   : Integer.parseInt(tokens[9]),
                        'reads_span' :Integer.parseInt(tokens[11]),
                        'reads_junction':Integer.parseInt(tokens[12]),
                        'soap_type'  :tokens[13],
                        'is_in_frame':tokens[14] == 'inframe-shift',
                        'assay_id'   : sampleMapping[tokens[0]]])
                totalUpdated++;
                if (totalUpdated % 100 == 0) {
                    print('.')
                }

            }
            writeAudit('Inserted soap junctions',totalUpdated,stepCt,'Done');
            println('')
            println("Successfully inserted $totalUpdated events")

        }
        catch (BatchUpdateException ex) {
            throw ex.getNextException()
        }
        stepCt++;

    }

    private static void importTophatJunctions() {
        //withBatch on the post fusion file, each line specifies one junction which creates one fusion
        def totalUpdated = 0;
        try {
            new File(options.tophatFusionPost).eachLine {line ->
                def tokens = line.split();
                //                        columns:
                //                        1. Sample name in which a fusion is identified
                //                        2. Gene on the "left" side of the fusion
                //                        3. Chromosome ID on the left
                //                        4. Coordinates on the left
                //                        5. Gene on the "right" side
                //                        6. Chromosome ID on the right
                //                        7. Coordinates on the right
                //                        8. Number of spanning reads
                //                        9. Number of spanning mate pairs
                //                        10. Number of spanning mate pairs where one end spans a fusion
                deapp.execute(queryCombined, [
                        'up_gene'    : tokens[1],
                        'up_chr'     : tokens[2],
                        'up_pos'     : Integer.parseInt(tokens[3]),
                        'up_strand'  : null,
                        'up_end'     : Integer.parseInt(tokens[3]),
                        'down_gene'  : tokens[4],
                        'down_chr'   : tokens[5],
                        'down_pos'   : Integer.parseInt(tokens[6]),
                        'down_strand': null,
                        'down_end'   : Integer.parseInt(tokens[6]),
                        'reads_junction':Integer.parseInt(tokens[7]),
                        'pairs_span' :Integer.parseInt(tokens[8]),
                        'pairs_junction':Integer.parseInt(tokens[9]),
                        'assay_id'   : sampleMapping[tokens[0]]])
                totalUpdated++;
                if (totalUpdated % 100 == 0) {
                    print('.')
                }

            }
            writeAudit('Inserted tophat junctions',totalUpdated,stepCt,'Done');
            println('')
            println("Successfully inserted $totalUpdated events")

        }
        catch (BatchUpdateException ex) {
            throw ex.getNextException()
        }
        stepCt++;

    }

    private static Boolean parseOptions(args) {
        def cli = new CliBuilder(usage: '-[hscdiatmoxjuf] ',
                posix:false,
                header: 'Parameters marked with asterisk * are mandatory')
        // Create the list of options.
        cli.with {
            h longOpt: 'help', 'Show usage information'
            s longOpt: 'studyId', args: 1, argName: 'studyId', required:true, '* Study ID under which two region data should be imported'
            c longOpt: 'conceptPath', args: 1, argName: 'conceptCode', required:true, '* Concept path where should the data be stored'
            d longOpt: 'datasource', args: 1, argName: 'datasource', required:true, '* Identification of the dataset this data comes from'
            t longOpt: 'tophatFusionPost', args: 1, argName: 'tophatFile', 'Path to the result of tophat fusion-post'
            o longOpt: 'soapFusion', args: 1, argName: 'soapFile', 'Path to file containing combined SOAPFuse fusion files'
//            x longOpt: 'soapFusionDir', args: 1, argName: 'soapDir', 'Path to directory containing SOAPFuse fusion files with sampleID in their name'
            m longOpt: 'mapping', args: 1, argName: 'mappingFile', '(TopHat andd SOAPFuse only) Path to the subject sample mapping file (subjectID;sampleID)'
            j longOpt: 'cgaJunctions', args: 1, argName: 'junctionFile', 'Path to file containing output of CGA tools, junction file, needs to be used with cga-events, sample-id and subject-id'
            u longOpt: 'cgaEvents', args: 1, argName: 'eventsFile', 'Path to file containing output of CGA tools, events file, needs to be used with cga-junctions, sample-id and subject-id'
            i longOpt: 'subjectId', args: 1, argName: 'subjectId', '(for CGA only) ID of the subject to which this sample belongs to'
            a longOpt: 'sampleId', args: 1, argName: 'sampleId', '(for CGA only) ID of the sample'
            f longOpt: 'force',   'Answer yes to all prompts'
            r longOpt: 'ref', args: 1, argName: 'ref', 'Human genome reference version (i.e. hg18)'
        }
        cli.footer = "Example:\n-s example_study -c \"/Public studies/example_study/soap\" -d s --mapping sample/tworegion/mapping.txt --soapFusion sample/tworegion/soapresult.txt  \n" +
                "\n" +
                "-s example_study -c \"/Public studies/example_study/tophat\" -d t --mapping sample/tworegion/mapping.txt --tophatFusionPost sample/tworegion/tophatresults.txt  \n" +
                "\n" +
                "-s example_study -c \"/Public studies/example_study/cga\" -d b --cgaJunctions sample/tworegion/cga_junctions.csv  --cgaEvents sample/tworegion/cga_events.tsv -i subject1 -a sample1"
        options = cli.parse(args)
        // Show usage text when -h or --help option is used.
        if (!options) {
            return false
        };
        if  (options.h) {
        } else if (options.t && !options.m){
            println('for tophat a mapping file is needed');
        } else if ((options.o || options.x) && !options.m){
            println('for SOAPFuse a mapping file is needed');
        } else if ((options.j == false) != (options.u == false)){
            println('for CGA both the junction and events file is needed');
        } else if (options.j && options.u && (!options.i ||  !options.a)){
            println('for CGA both the sample id and subject id has to be specified');
        } else if ((!options.t || !options.m) && ((!options.o && !options.x) || !options.m) && (!options.j || !options.u)){
            println('one of tophat, cga or soapfuse input files must be specified');
        } else if (options.r && (options.r != 'hg17' || options.r != 'hg18' || options.r != 'hg19')) {
            println('unknown reference');
        } else {
            if (options.t && !new File(options.t).exists()) {
                println('tophat fusion post doesn\'t exist!');
                return false;
            }
            if (options.m && !new File(options.m).exists()) {
                println('mapping file doesn\'t exist!');
                return false;
            }
            if (options.o && !new File(options.o).exists()) {
                println('SOAPFuse file doesn\'t exist!');
                return false;
            }
            if (options.x && !new File(options.x).exists()) {
                println('SOAPFuse directory doesn\'t exist!');
                return false;
            }
            if (options.j && !new File(options.j).exists()) {
                println('CGA junction file doesn\'t exist!');
                return false;
            }
            if (options.u && !new File(options.u).exists()) {
                println('CGA events file doesn\'t exist!');
                return false;
            }
            return true;
        }
        cli.usage();
        return false;
    }

    private static void insertCgaJunctions() {
        String platform;
        try {
            def updatedCounts = deapp.withBatch(100, "INSERT INTO deapp.de_two_region_junction\
             (up_chr, up_pos, up_strand, up_end, down_chr, down_pos, down_strand, down_end, is_in_frame, external_id, assay_id) VALUES \
            (:up_chr,:up_pos,:up_strand,:up_end,:down_chr,:down_pos,:down_strand,:down_end, null,       :external_id,:assay_id)", {
                BatchingPreparedStatementWrapper it ->
                    new File(options.cgaJunctions).eachLine {line ->
                        if (line[0] == '#' || line[0] == ' ' || line[0] == '>') {
                            //header
                            if (line.startsWith('#GENOME_REFERENCE\t')) {
                                platform = line.replaceFirst('#GENOME_REFERENCE\t', '')
                            }
                            return;
                        }
                        def tokens = line.split();
                        // Id            LeftChr LeftPosition       LeftStrand          LeftLength         RightChr              RightPosition     RightStrand                RightLength

                        it.addBatch([
                                'external_id': Integer.parseInt(tokens[0]),
                                'up_chr'     : tokens[1],
                                'up_pos'     : Integer.parseInt(tokens[2]),
                                'up_strand'  : tokens[3],
                                'up_end'     : Integer.parseInt(tokens[2])+Integer.parseInt(tokens[4]),
                                'down_chr'   : tokens[5],
                                'down_pos'   : Integer.parseInt(tokens[6]),
                                'down_strand': tokens[7],
                                'down_end'   :  Integer.parseInt(tokens[6])+Integer.parseInt(tokens[8]),
                                'assay_id'   : assayId])
                    }
            })
            def totalUpdated = (updatedCounts as Integer[]).sum();
            writeAudit('Inserted cga junctions',totalUpdated,stepCt,'Done');
        }
        catch (BatchUpdateException ex) {
            throw ex.getNextException() //real exception is in the next one, container exception is just "batch failed"
        }
    }

    private static void insertCgaEvents() {
        //create a map of external id - id
        Map<Integer, Integer> internalIds = deapp.rows("select external_id, two_region_junction_id from deapp.de_two_region_junction where assay_id=$assayId").collectEntries {
            [(it[0]): it[1]]
        }

        Integer eventsCount = 0, junctionEventsCount = 0, genesCount = 0;
        new File(options.cgaEvents).eachLine {line ->
            if (line.length() == 0 || line[0] == '#' || line[0] == ' ' || line[0] == '>') {
                //header
                return;
            }

            def tokens = line.split('\t', -1);   //-1 means we want to see trailing empty columns as well
            //EventId	Type	RelatedJunctionIds	MatePairCounts	FrequenciesInBaselineGenomeSet	OriginRegionChr	OriginRegionBegin	OriginRegionEnd	OriginRegionLength	OriginRegionStrand	DestinationRegionChr	DestinationRegionBegin	DestinationRegionEnd	DestinationRegionLength	DestinationRegionStrand	DisruptedGenes	ContainedGenes	GeneFusions	RelatedMobileElement	MobileElementChr	MobileElementBegin	MobileElementEnd	MobileElementStrand

            GString query = "INSERT INTO deapp.de_two_region_event( \
                                         cga_type, soap_class) \
                                 VALUES (${tokens[1]}, null)"
            deapp.execute(query);
            eventsCount++;
            if (eventsCount % 100 == 0) {
                print('.')
            }

            def junctions = tokens[2].split(';');
            def matePairCounts = tokens[3].split(';');
            def baseFreqs = tokens[4].split(';');
            for (int i = 0; i < junctions.length; i++) {
                def junction = Long.parseLong(junctions[i]);
                def matePairCount = Integer.parseInt(matePairCounts[i]);
                def baseFreq = Double.parseDouble(baseFreqs[i]);
                if (isOracle) {
                    query = "INSERT INTO deapp.de_two_region_junction_event( \
                                             junction_id, event_id,  \
                                             reads_span, reads_junction, pairs_span, pairs_junction,  \
                                             pairs_end, reads_counter, base_freq) \
                                     VALUES (${internalIds[junction]}, de_two_region_event_seq.currval, null, null, $matePairCount, null, null, null, $baseFreq )"
                } else {
                    query = "INSERT INTO deapp.de_two_region_junction_event( \
                                             junction_id, event_id,  \
                                             reads_span, reads_junction, pairs_span, pairs_junction,  \
                                             pairs_end, reads_counter, base_freq) \
                                     VALUES (${internalIds[junction]}, currval( 'de_two_region_event_seq'), null, null, $matePairCount, null, null, null, $baseFreq )"
                }
                deapp.execute(query);
                junctionEventsCount++;
            }

            /* TODO: integrate with genes */
            def disruptedGgenes = tokens[15].split(';').findAll({ it != '' && !it.matches('^LOC[0-9]+')});
            genesCount += insertGenes(disruptedGgenes, 'DISRUPTED')
            def containedGenes = tokens[16].split(';').findAll({ it != '' && !it.matches('^LOC[0-9]+')});
            genesCount += insertGenes(containedGenes, 'CONTAINED')
            def fusionGenes = tokens[17]
                                .replaceAll('TSS-UPSTREAM\\[','') //we don't care the genes are fused in a TFBS
                                .replaceAll('\\]/','/')
                                .split('[;/]')
                                .findAll({  it != '' && !it.matches('^LOC[0-9]+')})
                                .unique();
            genesCount += insertGenes(fusionGenes, 'FUSION')
        }
        writeAudit('Inserted cga events',eventsCount,stepCt,'Done');
        writeAudit('Inserted cga junction-event links',junctionEventsCount,stepCt,'Done');
        writeAudit('Inserted cga gene-event links',genesCount,stepCt,'Done');
        println('')
        println("Successfully inserted $eventsCount events")
    }


    private static void getBiomarkerId(String hugo) {
        String query =     ''
        //TODO: get entrez id from hugo
    }

    private static int insertGenes(ArrayList<String> genes, String type) {
        int genesCount = 0;
        GString query
        for (int i = 0; i < genes.size(); i++) {
            String geneId = genes[i];
            if (isOracle) {
            query = "INSERT INTO deapp.de_two_region_event_gene( \
                gene_id, event_id, effect) VALUES   \
               ($geneId, de_two_region_event_seq.currval, $type)";
            } else {
                query = "INSERT INTO deapp.de_two_region_event_gene( \
                gene_id, event_id, effect) VALUES   \
               ($geneId, currval( 'de_two_region_event_seq'), $type)";
            }
            deapp.execute(query);
            genesCount++;
        }
        genesCount
    }


}

