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


package org.transmartproject.pipeline.transmart

import groovy.sql.GroovyRowResult
import groovy.sql.Sql;

import org.apache.log4j.Logger;

class BioDataExtCode {

    private static final Logger log = Logger.getLogger(BioDataExtCode)

    Sql biomart

    /**
     *
     * @param extCodeMap code -> bio_data_type	bio_data_ext_code_id
     */
    void loadBioDataExtCode(Map extCodeMap) {

        String code, dataType, qry
        long bioDataId

        extCodeMap.each {k, v ->
            code = k
            dataType = v.split("\t")[0]
            bioDataId = Integer.parseInt(v.split("\t")[1])

            if (isBioDataExtCodeExist(dataType, bioDataId)) {
                log.info "(\"$code\", $dataType) already exists in BIO_DATA_EXT_CODE ..."
            } else {
                log.info "Load (\"$code\", $dataType) into BIO_DATA_EXT_CODE ..."

                qry = """ insert into bio_data_ext_code(code, code_source, code_type, bio_data_type, bio_data_id) values(?, ?, ?, ?, ?) """
                biomart.execute(qry, [
                        code,
                        'Alias',
                        'SYNONYM',
                        dataType,
                        bioDataId
                ])
            }
        }
    }


    boolean insertBioDataExtCode(bioMarkerID, synonym, dataType) {
        biomart.executeInsert("""
                insert into BIO_DATA_EXT_CODE(BIO_DATA_ID, CODE, CODE_TYPE, BIO_DATA_TYPE, CODE_SOURCE)
                  values(:bio_data_id, :code, :code_type, :bio_data_type, :code_source)
                """,
                [bio_data_id: bioMarkerID,
                        code: synonym,
                        code_type: 'SYNONYM',
                        bio_data_type: 'BIO_MARKER.' + dataType,
                        code_source: 'Alias'])
    }

    boolean isBioDataExtCodeExist(long bioDataId) {
        String qry = "select count(1) from bio_data_ext_code where bio_data_id=?"
        GroovyRowResult rowResult = biomart.firstRow(qry, [bioDataId])
        int count = rowResult[0]
        return count > 0
    }


    boolean isBioDataExtCodeExist(String dataType, long bioDataId) {
        String qry = "select count(1) from bio_data_ext_code where bio_data_type=? and bio_data_id=?"
        GroovyRowResult rowResult = biomart.firstRow(qry, [dataType, bioDataId])
        int count = rowResult[0]
        return count > 0
    }


    boolean isBioDataExtCodeExist(long bioDataId, String synonym) {
        String qry = "select count(*) from BIO_DATA_EXT_CODE where BIO_DATA_ID=? and CODE=?"
        GroovyRowResult res = biomart.firstRow(qry, [bioDataId, synonym])
        int count = res[0]
        return count > 0
    }


    void setBiomart(Sql biomart) {
        this.biomart = biomart
    }
}
