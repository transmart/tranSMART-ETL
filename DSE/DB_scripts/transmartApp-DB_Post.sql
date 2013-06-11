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
      
END $$