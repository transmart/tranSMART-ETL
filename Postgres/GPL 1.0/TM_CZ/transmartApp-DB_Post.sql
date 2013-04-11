/*************************************************************************
* Copyright 2008-2012 ????
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************/

/*****************************************************************
* This script is ment to fix a few short comings in the TranSMART
* Postgres database. These changes are needed to run the Postgres 
* function "tm_cz.i2b2_load_clinical_data" (part of the clinical-data
* ETL-upload pipeline) successfully.
* It is a temporary solution. It should be fixed in the 
* "transmartApp-DB" repository eventually.
*****************************************************************/

DO $$

DECLARE pat_start       integer;

BEGIN
    -- Define 'patient_num' sequence. 
    -- Needed to fill 'i2b2demodata.patient_dimension'
    BEGIN
        CREATE sequence i2b2demodata.seq_patient_num;
        -- There is already data 'i2b2demodata.patient_dimension'
        SELECT max(i2b2demodata.seq_patient_num) into pat_start from i2b2demodata.patient_dimension;
        PERFORM setval ('i2b2demodata.seq_patient_num', pat_start + 1);
    EXCEPTION
        WHEN others THEN RAISE NOTICE 'sequence "i2b2demodata.seq_patient_num" alread exists';
    END;
 
  -- define 'concept_id' sequence to fill 'concept_dimension.concept_cd'
  -- Within transmart we use 'concept_cd' different than meant by i2b2?
    BEGIN
        CREATE sequence i2b2demodata.concept_id; 
        -- There is already data in 'i2b2demodate.concept_dimension.concept_cd' not only numbers
        PERFORM setval ('i2b2demodata.concept_id', 1000000);
    EXCEPTION
        WHEN others THEN RAISE NOTICE 'sequence "i2b2demodata.concept_id" already exists';
    END;

  -- Add column 'data_type' to table 'tm_wz.wt_trial_nodes'
    BEGIN
        ALTER TABLE tm_wz.wt_trial_nodes ADD COLUMN data_type character varying(20);
    EXCEPTION
        WHEN duplicate_column THEN RAISE NOTICE 'column "data_type" already exists in table "tm_wz.wt_trial_nodes"';
    END;

  -- in Table "searchapp.plugin_module" the "heatmap" definition doesnot seems to be right
  -- Also some definitions in this table seems to be missing.
    BEGIN
        update searchapp.plugin_module set params = '{"id":"heatmap","converter":{"R":["source(''||PLUGINSCRIPTDIRECTORY||Common/dataBuilders.R'')","source(''||PLUGINSCRIPTDIRECTORY||Common/ExtractConcepts.R'')","source(''||PLUGINSCRIPTDIRECTORY||Common/collapsingData.R'')","source(''||PLUGINSCRIPTDIRECTORY||Common/parseDirectory.R'')","source(''||PLUGINSCRIPTDIRECTORY||Heatmap/BuildHeatmapData.R'')","\t\t\t\t\t\tparseDirectory(topLevelDirectory = ''||TOPLEVELDIRECTORY||'')\n\t\t\t\t\t","\t\t\t\t\t\t\tHeatmapData.build(input.gexFile = ''||TOPLEVELDIRECTORY||/workingDirectory/mRNA.trans'',\n\t\t\t\t\t\t\tgenes = ''||GENES||'',\n\t\t\t\t\t\t\tgenes.aggregate = ''||AGGREGATE||'',\n\t\t\t\t\t\t\tsample.subset1=''||SAMPLE1||'',\n\t\t\t\t\t\t\ttime.subset1=''||TIMEPOINTS1||'',\n\t\t\t\t\t\t\ttissues.subset1=''||TISSUES1||'',\n\t\t\t\t\t\t\tplatform.subset1=''||GPL1||'',\n\t\t\t\t\t\t\tsample.subset2=''||SAMPLE2||'',\n\t\t\t\t\t\t\ttime.subset2=''||TIMEPOINTS2||'',\n\t\t\t\t\t\t\ttissues.subset2=''||TISSUES2||'',\n\t\t\t\t\t\t\tplatform.subset2=''||GPL2||'')\n\t\t\t\t\t"]},"name":"Heatmap","dataFileInputMapping":{"CLINICAL.TXT":"FALSE","SNP.TXT":"snpData","MRNA_DETAILED.TXT":"TRUE"},"dataTypes":{"subset1":["CLINICAL.TXT"]},"pivotData":false,"view":"Heatmap","processor":{"R":["source(''||PLUGINSCRIPTDIRECTORY||Heatmap/HeatmapLoader.R'')","\t\t\t\t\tHeatmap.loader(\n\t\t\t\t\tinput.filename=''outputfile''\n\t\t\t\t\t)\n\t\t\t\t\t"]},"renderer":{"GSP":"/RHeatmap/heatmapOut"},"variableMapping":{"||GENES||":"divIndependentVariablePathway","||AGGREGATE||":"divIndependentVariableprobesAggregation","||TIMEPOINTS1||":"divIndependentVariabletimepoints","||TISSUES1||":"divIndependentVariabletissues","||SAMPLE1||":"divIndependentVariablesamples","||GPL1||":"divIndependentVariablegplsValue","||TIMEPOINTS2||":"divIndependentVariabletimepoints2","||TISSUES2||":"divIndependentVariabletissues2","||SAMPLE2||":"divIndependentVariablesamples2","||GPL2||":"divIndependentVariablegplsValue2"}}' where module_seq =33;
    EXCEPTION
        WHEN others THEN RAISE NOTICE 'Problems updating table "searchapp.plugin_module" (column "params" for heatmap)';
    END;

      
END $$
