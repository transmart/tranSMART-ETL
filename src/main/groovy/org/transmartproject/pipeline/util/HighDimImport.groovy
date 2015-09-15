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
package org.transmartproject.pipeline.util

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import org.codehaus.groovy.syntax.Numbers
import org.transmartproject.pipeline.tworegion.TwoRegion

import java.sql.SQLException

/**
 * Created by j.hudecek on 2-9-2014.
 */
class HighDimImport {
    protected static final Logger log = Logger.getLogger(TwoRegion)
    protected static OptionAccessor options

    //DBs
    protected static Sql deapp
    protected static Sql i2b2demodata
    protected static Sql i2b2metadata
    protected static Sql tm_lz
    protected static Sql tm_cz

    //per run vars
    protected static String datasetId
    protected static String sampleId
    protected static String concept
    protected static String subjectId

    //processed vars
    protected static String sourceSystem
    protected static String procedureName
    protected static int stepCt

    //DB IDs
    protected static int jobId
    private static Integer conceptCd
    private static BigDecimal patientNum
    protected static Integer assayId

    //mapping from sampleID to assayID
    protected static HashMap<String, String> sampleMapping = new HashMap<>();

    protected static boolean isOracle;
    protected static String executeSP;

    protected static void initDB() {
        PropertyConfigurator.configure("conf/log4j.properties");

        log.info("Start loading property file Common.properties ...")
        Properties props = Util.loadConfiguration("conf/Common.properties");

        isOracle = props.getProperty("url","").contains('oracle');
        executeSP = isOracle ? "call" : "select"

        deapp = Util.createSqlFromPropertyFile(props, "deapp")
        i2b2demodata = Util.createSqlFromPropertyFile(props, "i2b2demodata")
        i2b2metadata = Util.createSqlFromPropertyFile(props, "i2b2metadata")
        tm_lz = Util.createSqlFromPropertyFile(props, "tm_lz")
        tm_cz = Util.createSqlFromPropertyFile(props, "tm_cz")
    }

    protected static void endAudit(String status) {
        if (isOracle) {
            tm_cz.execute("call tm_cz.cz_end_audit ($jobId, $status)");
        } else {
            tm_cz.execute("select tm_cz.cz_end_audit ($jobId, $status)");
        }
    }

    protected static void writeAudit(String desc, int recordsaffected, int step, String status ) {
        if (isOracle) {
            tm_cz.execute(executeSP+" tm_cz.cz_write_audit($jobId,'TM_CZ','$procedureName','$desc',$step,$recordsaffected,'$status')");
        } else {
            tm_cz.execute("select tm_cz.cz_write_audit($jobId,'TM_CZ',$procedureName,$desc,$step,$recordsaffected,$status)");
        }
    }


    protected static void cleanupOptions() {
        datasetId = options.studyId;
        concept = options.conceptPath

        datasetId = datasetId.replace("'", "''");
        concept = concept.replace("'", "''");

        concept = concept.replace('/', '\\').replace('_', ' ').replace('%', 'Pct').replace('&', ' and ').replace('+', ' and ');
        if (!concept.startsWith('\\')) {
            concept = '\\' + concept
        };
        if (!concept.endsWith('\\')) {
            concept += '\\'
        };
    }

    protected static void startAudit() {
        //Audit JOB Initialization
        if (isOracle) {
            tm_cz.call("call tm_cz.cz_start_audit (?, 'TM_CZ',?)", [procedureName, Sql.INTEGER], { jobid -> jobId = jobid });
        } else {
            jobId = tm_cz.firstRow("select tm_cz.cz_start_audit ($procedureName, 'TM_CZ')")[0];
        }
        writeAudit('Starting metadata creation',0,1,'Done');
    }

    protected static void insertMetadata() {
        insertPatient()
        insertSubjectSampleMapping()
        insertObservationFact()
        insertSampleDimension()
    }

    protected static String readMappingFile(mapping) {
        //line by line on the sample subject mapping file, creating patients, concepts,...
        new File(mapping).eachLine {line ->
            if (line.indexOf(';') == -1) {
                throw new IllegalArgumentException("Mapping file is unknown format. Please use subjectID;sampleID per line")
            };
            subjectId = line.split(';')[0];
            sampleId = line.split(';')[1];

            if (subjectId == '' || sampleId == '') {
                throw new IllegalArgumentException("Mapping file is invalid on line '$line' Please use subjectID;sampleID per line")
            };
            initSourceSystem(subjectId, sampleId)
            insertMetadata()
            sampleMapping.put(sampleId, assayId);
        }
    }

    protected static void insertConceptPath() {
        Integer topLevel = concept.length() - concept.replace('\\', '').length();
        if (topLevel < 3) {
            throw new IllegalArgumentException("Path specified in top_node must contain at least 2 nodes");
        }
        def pathEls = concept.split('\\\\');
        String path = "\\"
        for (Integer level = 1; level < topLevel; level++) {
            String node = pathEls[level];
            path = path + node + "\\";
            String attr = (level == topLevel - 1) ? "LAH" : "FA";
            conceptCd = insertConceptNode(path, node, level, attr)
        }
    }

    protected static void initSourceSystem(String sampleId, String subjectId) {
        sampleId = sampleId.replace("'", "''");
        subjectId = subjectId.replace("'", "''");

        sourceSystem = "${datasetId}:${sampleId}";
    }

    protected static void handleError(Exception ex) {
        ex.printStackTrace()
        tm_cz.execute(executeSP+" tm_cz.cz_write_error($jobId, '1', '${ex.message}', '${ex.toString()}' ,'')");
        endAudit('FAIL');
    }

    private static void insertSampleDimension() {
        if (isOracle) {
            i2b2metadata.execute("merge into i2b2demodata.sample_dimension c  " +
                    "using (select '$sampleId' as sampleId,'$patientNum' as patientNum  from dual) d " +
                    "       on (c.sample_cd =d.sampleId) " +
                    "when not matched then "+
                    "INSERT (sample_cd) \
                                                                 VALUES ('$sampleId')");
            writeAudit('Merged sample_dimension, metadata done', 1, stepCt, 'Done');
        } else {
            if (!i2b2demodata.executeInsert("INSERT INTO i2b2demodata.sample_dimension(sample_cd) \
                                                SELECT $sampleId  where NOT EXISTS(SELECT NULL FROM i2b2demodata.sample_dimension WHERE sample_cd=$sampleId );").empty)
                writeAudit('Inserted sample_dimension, metadata done', 1, stepCt, 'Done');
        }
        stepCt++;

    }

    private static void insertObservationFact() {
        if (isOracle) {
            i2b2metadata.execute("merge into i2b2demodata.observation_fact c  " +
                    "using (select '$conceptCd' as concept_code,'$patientNum' as patientNum  from dual) d " +
                    "       on (c.concept_cd =d.concept_code and c.patient_num = d.patientNum) " +
                    "when not matched then "+
                    "insert  \
                             (encounter_num \
                                 ,patient_num \
                                 ,concept_cd \
                                 ,start_date \
                                 ,modifier_cd \
                                 ,valtype_cd \
                                 ,tval_char \
                                 ,sourcesystem_cd \
                                 ,import_date \
                                 ,valueflag_cd \
                                 ,provider_id \
                                 ,location_cd \
                                 ,units_cd \
                                 ,instance_num \
                             ) \
                             values  ($patientNum \
                                           ,$patientNum \
                                           ,$conceptCd \
                                           ,current_timestamp \
                                           ,'@' \
                                           ,'T' \
                                           ,'E'  \
                                           ,'$datasetId' \
                                           ,current_timestamp \
                                           ,'@' \
                                           ,'@' \
                                           ,'@' \
                                           ,'' \
                                           , 0)");
            writeAudit('Merged observation_fact', 1, stepCt, 'Done');
        } else {
            if (!i2b2demodata.executeInsert("insert into i2b2demodata.observation_fact \
                             (encounter_num \
                                 ,patient_num \
                                 ,concept_cd \
                                 ,start_date \
                                 ,modifier_cd \
                                 ,valtype_cd \
                                 ,tval_char \
                                 ,sourcesystem_cd \
                                 ,import_date \
                                 ,valueflag_cd \
                                 ,provider_id \
                                 ,location_cd \
                                 ,units_cd \
                                 ,instance_num \
                             ) \
                             SELECT  $patientNum \
                                           ,$patientNum \
                                           ,$conceptCd \
                                           ,current_timestamp \
                                           ,'@' \
                                           ,'T' \
                                           ,'E'  \
                                           ,$datasetId \
                                           ,current_timestamp \
                                           ,'@' \
                                           ,'@' \
                                           ,'@' \
                                           ,'' \
                                           , 0 \
                            where not exists (select null from i2b2demodata.observation_fact where $conceptCd =concept_cd and $patientNum = patient_num) ").empty) {
                /* T stands for Text data type*/
                /* E Stands for Equals for Text Types*/
                writeAudit('Inserted observation_fact', 1, stepCt, 'Done');
            }
        }
        stepCt++;
    }

    private static void insertSubjectSampleMapping() {
        //first try to insert the mapping, if it exists get its num. We assume it usually won't exist
        if (isOracle) {
            i2b2metadata.execute("merge into deapp.de_subject_sample_mapping c  " +
                    "using (select '$conceptCd' as concept_code,'$subjectId' as subjectId  from dual) d " +
                    "       on (c.concept_code=d.concept_code and c.subject_id = d.subjectId) " +
                    "when not matched then " +
                    "insert (assay_id,\
                                    patient_id, \
                                    subject_id, \
                                    concept_code, \
                                    trial_name, \
                                    platform) \
                           Values (DEAPP.SEQ_ASSAY_ID.nextval,\
                                      $patientNum, \
                                     '$subjectId', \
                                     '$conceptCd', \
                                     '$datasetId', \
                                     'two_region')");
            assayId = i2b2demodata.firstRow("SELECT assay_id FROM deapp.de_subject_sample_mapping WHERE concept_code = $conceptCd  AND subject_id=$subjectId ")[0];
            stepCt++;
            writeAudit('Merged de_subject_sample_mapping', 1, stepCt, 'Done');
        } else {
            def inserted = deapp.executeInsert("insert into deapp.de_subject_sample_mapping (\
                                    patient_id, \
                                    subject_id, \
                                    assay_id, \
                                    concept_code, \
                                    trial_name, \
                                    platform) \
                                                         select $patientNum, \
                                                         $subjectId, \
                                                         nextval( 'deapp.seq_assay_id' ), \
                                                         $conceptCd, \
                                                         $datasetId, \
                                                         'two_region'\
                                            WHERE NOT EXISTS ( SELECT NULL FROM deapp.de_subject_sample_mapping WHERE concept_code = $conceptCd  AND subject_id=$subjectId  );\n");
            if (inserted.empty) {
                assayId = i2b2demodata.firstRow("SELECT assay_id FROM deapp.de_subject_sample_mapping WHERE concept_code = $conceptCd  AND subject_id=$subjectId ")[0];
            } else {
                assayId = inserted[0][5];
                //I'm not sure why it is on the second position, better check we really have the right patient_num:
                if (null == i2b2demodata.firstRow("SELECT assay_id FROM deapp.de_subject_sample_mapping WHERE concept_code = $conceptCd  AND subject_id=$subjectId  AND assay_id=$assayId ")) {
                    throw new SQLException("Getting assay_id failed")
                };
                stepCt++;
                writeAudit('Inserted de_subject_sample_mapping', 1, stepCt, 'Done');
            }
        }
    }

    private static void insertPatient() {
        //first try to insert the patient, if it exists get its num. We assume it usually won't exist
        if (isOracle) {
            i2b2metadata.execute("merge into i2b2demodata.patient_dimension c  " +
                    "using (select '$sourceSystem' as sourceSystem from dual) d " +
                    "       on (c.sourcesystem_cd=d.sourceSystem) " +
                    "when not matched then " +
                    "insert  \
                                                 (sex_cd, \
                                                         age_in_years_num, \
                                                         race_cd, \
                                                         update_date, \
                                                         download_date, \
                                                         import_date, \
                                                         sourcesystem_cd \
                                                 ) \
                                           VALUES ('Unknown', \
                                                         0, \
                                                         null, \
                                                         current_timestamp, \
                                                         current_timestamp, \
                                                         current_timestamp, \
                                                         '$sourceSystem')");
            patientNum = new BigDecimal(i2b2demodata.firstRow("SELECT patient_num FROM i2b2demodata.patient_dimension WHERE sourcesystem_cd = $sourceSystem ")[0]);
            stepCt++;
            writeAudit('Merged patient ' + sourceSystem, 0, stepCt, 'Done');
        } else {
            def inserted = i2b2demodata.executeInsert("insert into i2b2demodata.patient_dimension \
                                                 (patient_num, \
                                                         sex_cd, \
                                                         age_in_years_num, \
                                                         race_cd, \
                                                         update_date, \
                                                         download_date, \
                                                         import_date, \
                                                         sourcesystem_cd \
                                                 ) \
                                           select nextval('i2b2demodata.seq_patient_num'), \
                                                         'Unknown', \
                                                         0, \
                                                         null, \
                                                         current_timestamp, \
                                                         current_timestamp, \
                                                         current_timestamp, \
                                                         $sourceSystem\
                                            WHERE NOT EXISTS ( SELECT NULL FROM i2b2demodata.patient_dimension WHERE sourcesystem_cd = $sourceSystem  );\n")
            if (inserted.empty) {
                patientNum = new BigDecimal(i2b2demodata.firstRow("SELECT patient_num FROM i2b2demodata.patient_dimension WHERE sourcesystem_cd = $sourceSystem ")[0]);
            } else {
                patientNum = new BigDecimal(inserted[0][0]);
                //I'm not sure why it is on the second position, better check we really have the right patient_num:
                if (null == i2b2demodata.firstRow("SELECT patient_num FROM i2b2demodata.patient_dimension WHERE sourcesystem_cd = $sourceSystem  AND patient_num=$patientNum ")) {
                    throw new SQLException("Getting patient_num failed")
                };
                stepCt++;
                writeAudit('Added patient ' + sourceSystem, 0, stepCt, 'Done');
            }
        }
    }

    private static int insertConceptNode(String path, String node, int level, String attr) {
        Integer conceptId;
        //don't insert root node ("Public Studies") with a specific dataset id
        String sourcesystem = level == 1 ? null : datasetId;
        String comment = "Trial:"+datasetId;
        //first try to insert concept, if it exists get its cd. We assume it usually won't exist
        if (isOracle) {
            i2b2demodata.execute("merge into i2b2demodata.concept_dimension c  " +
                    "using (select '$path' as path from dual) d " +
                    "       on (c.concept_path=d.path) " +
                    "when not matched then " +
                    "       insert  (concept_path, name_char, update_date, download_date, import_date, sourcesystem_cd)" +
                    "        values ('$path','$node',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,null)");
            stepCt++;
            writeAudit('Merged concept '+path,0,stepCt ,'Done');
        } else {
            List inserted;
            inserted = i2b2demodata.executeInsert("insert into i2b2demodata.concept_dimension (concept_path, name_char, update_date, download_date, import_date, sourcesystem_cd)\
                    SELECT $path,$node,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,null  \
                    WHERE NOT EXISTS ( SELECT NULL FROM i2b2demodata.concept_dimension WHERE concept_path = $path )")
            if (inserted.empty) {
            } else {
                conceptId = Integer.parseInt(inserted[0][0]);
                //I'm not sure why it is on the second position, better check we really have the right concept_cd:
                if (null == i2b2demodata.firstRow("SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE concept_path = $path AND concept_cd=$conceptId")) {
                    throw new SQLException("Getting concept_cd failed")
                };
                stepCt++;
                writeAudit('Added concept '+path,0,stepCt ,'Done');
            }
        }
        conceptId = Integer.parseInt(i2b2demodata.firstRow("SELECT concept_cd FROM i2b2demodata.concept_dimension WHERE concept_path = $path")[0]);
        if (isOracle) {
            i2b2metadata.execute("merge into i2b2metadata.i2b2 c  " +
                    "using (select '$path' as path from dual) d " +
                            "       on (c.c_fullname=d.path) " +
                            "when not matched then " +
                            "       insert  \
                                                 (c_hlevel \
                                                  ,c_fullname \
                                                  ,c_name \
                                                  ,c_visualattributes \
                                                  ,c_synonym_cd \
                                                  ,c_facttablecolumn \
                                                  ,c_tablename \
                                                  ,c_columnname \
                                                  ,c_dimcode \
                                                  ,c_tooltip \
                                                  ,update_date \
                                                  ,download_date \
                                                  ,import_date \
                                                  ,sourcesystem_cd \
                                                  ,c_basecode \
                                                  ,c_operator \
                                                  ,c_columndatatype \
                                                  ,c_comment \
                                                  ,m_applied_path \
                                                  ,c_metadataxml \
                                                 ) \
                                 VALUES ($level \
                                         ,'$path' \
                                         ,'$node' \
                                         ,'$attr' \
                                         ,'N' \
                                         ,'CONCEPT_CD' \
                                         ,'CONCEPT_DIMENSION' \
                                         ,'CONCEPT_PATH' \
                                         ,'$path' \
                                         ,'$path' \
                                         ,current_timestamp \
                                         ,current_timestamp \
                                         ,current_timestamp \
                                         ,'$sourcesystem' \
                                         ,$conceptId \
                                         ,'LIKE' \
                                         ,'T' \
                                         ,'$comment' \
                                         ,'@' \
                                         ,null)")
            stepCt++;
            writeAudit('Merged i2b2 ' + path, 0, stepCt, 'Done');
        } else {
            if (!i2b2metadata.executeInsert("insert into i2b2metadata.i2b2 \
                                                 (c_hlevel \
                                                  ,c_fullname \
                                                  ,c_name \
                                                  ,c_visualattributes \
                                                  ,c_synonym_cd \
                                                  ,c_facttablecolumn \
                                                  ,c_tablename \
                                                  ,c_columnname \
                                                  ,c_dimcode \
                                                  ,c_tooltip \
                                                  ,update_date \
                                                  ,download_date \
                                                  ,import_date \
                                                  ,sourcesystem_cd \
                                                  ,c_basecode \
                                                  ,c_operator \
                                                  ,c_columndatatype \
                                                  ,c_comment \
                                                  ,m_applied_path \
                                                  ,c_metadataxml \
                                                 ) \
                                 select $level \
                                         ,$path \
                                         ,$node \
                                         ,$attr \
                                         ,'N' \
                                         ,'CONCEPT_CD' \
                                         ,'CONCEPT_DIMENSION' \
                                         ,'CONCEPT_PATH' \
                                         ,$path \
                                         ,$path \
                                         ,current_timestamp \
                                         ,current_timestamp \
                                         ,current_timestamp \
                                         ,$sourcesystem \
                                         ,$conceptId \
                                         ,'LIKE' \
                                         ,'T' \
                                         ,$comment \
                                         ,'@' \
                                         ,null\
                                WHERE NOT EXISTS ( SELECT NULL FROM i2b2metadata.i2b2 WHERE c_fullname = $path )").empty) {
                stepCt++;
                writeAudit('Added i2b2 ' + path, 0, stepCt, 'Done');
            }
        }
        return conceptId;
    }
}
