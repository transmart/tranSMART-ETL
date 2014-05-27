package org.transmartproject.pipeline.dictionary

import groovy.sql.Sql
import org.apache.log4j.Logger
import org.transmartproject.pipeline.transmart.BioDataCorrelDescr
import org.transmartproject.pipeline.transmart.BioDataCorrelation
import org.transmartproject.pipeline.transmart.BioMarker
import org.transmartproject.pipeline.util.Util

class CorrelationLoader {
    private static final Logger log = Logger.getLogger(CorrelationLoader)

    Sql sqlBiomart
    Sql sqlDeapp

    BioDataCorrelation bioDataCorrelation
    long bioDataCorrelDescrId1
    long bioDataCorrelDescrId2

    public CorrelationLoader(String correlation1, String correlation2) {
        log.info("Start loading property file ...")
        Properties props = Util.loadConfiguration('')
        sqlBiomart = Util.createSqlFromPropertyFile(props, "biomart")
        sqlDeapp = Util.createSqlFromPropertyFile(props, "deapp")

        bioDataCorrelation = new BioDataCorrelation()
        bioDataCorrelation.setBiomart(sqlBiomart)

        retrieveBioDataCorrelId(correlation1, correlation2)
    }

    public void close() {
        sqlBiomart.close()
        sqlDeapp.close()
    }

    /** Retrieves the bioDataCorrelDescrID or inserts a new one
     *  if it doesn't exist.
     */
    public void retrieveBioDataCorrelId(correlation1, correlation2) {

        // Set up BioDataCorrelDescr
        BioDataCorrelDescr bioDataCorrelDescr = new BioDataCorrelDescr()
        bioDataCorrelDescr.setBiomart(sqlBiomart)

        // Get or create data correlation description ids
        bioDataCorrelDescrId1 = getOrCreateBioDataCorrelId(bioDataCorrelDescr, correlation1)
        bioDataCorrelDescrId2 = getOrCreateBioDataCorrelId(bioDataCorrelDescr, correlation2)
    }

    private long getOrCreateBioDataCorrelId(BioDataCorrelDescr bioDataCorrelDescr, String correlation) {
        long bioDataCorrelDescrId = bioDataCorrelDescr.getBioDataCorrelId(correlation, correlation)
        if (bioDataCorrelDescrId == 0) {
            // Insert a new description
            bioDataCorrelDescr.insertBioDataCorrelDescr(correlation, "", correlation)
            // Get the id of the inserted entry
            bioDataCorrelDescrId = bioDataCorrelDescr.getBioDataCorrelId(correlation, correlation)
        }
        return bioDataCorrelDescrId
    }

    public boolean insertCorrelation(CorrelationEntry correlationEntry) {

        // Look up the BIO_MARKER_ID for symbol 1
        BioMarker bioMarker = new BioMarker()
        bioMarker.setBiomart(sqlBiomart)
        bioMarker.setOrganism(correlationEntry.organism)
        Long bioMarkerId1 = bioMarker.getBioMarkerIDBySymbol(correlationEntry.symbol1,
                                                             correlationEntry.markerType1)

        // Look up the BIO_MARKER_ID for symbol 2
        bioMarker = new BioMarker()
        bioMarker.setBiomart(sqlBiomart)
        bioMarker.setOrganism(correlationEntry.organism)
        Long bioMarkerId2 = bioMarker.getBioMarkerIDBySymbol(correlationEntry.symbol2,
                                                              correlationEntry.markerType2)

        // Add the correlation to BIO_DATA_CORRELATION if possible
        if (bioMarkerId1 != null && bioMarkerId2 != null) {
            bioDataCorrelation.insertBioDataCorrelation(bioMarkerId1, bioMarkerId2, bioDataCorrelDescrId1)
            bioDataCorrelation.insertBioDataCorrelation(bioMarkerId2, bioMarkerId1, bioDataCorrelDescrId2)
            return true;
        }
        return false;
    }
}
