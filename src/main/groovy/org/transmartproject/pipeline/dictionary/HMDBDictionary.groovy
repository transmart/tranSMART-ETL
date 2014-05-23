package org.transmartproject.pipeline.dictionary

import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Parses the metabolite dictionary and loads it into the database.
 */
class HMDBDictionary {
    private static final Logger log = Logger.getLogger(HMDBDictionary)

    static main(args) {
        if (!args) {
            println "HMDBDictionary <Metabolite.tsv>"
            System.exit(1)
        }

        def fileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties");
        File inputFile = new File(fileLocation)
        HMDBDictionary dict = new HMDBDictionary()
        dict.loadData(inputFile)
    }

    void loadData(File inputFile) {
        if (!inputFile.exists()) {
            log.error("File is not found: ${inputFile.getAbsolutePath()}")
            return
        }

        DictionaryLoader dictionaryLoader = new DictionaryLoader();

        try {
            inputFile.eachLine(0) { line, number ->
                if (number == 0) {
                    return // Skip the header line
                }

                // Split values
                String[] split = line.split("\t")
                String id = split[0]
                String name = split[1]

                // Construct entry
                BioMarkerEntry entry = new BioMarkerEntry("METABOLITE", "Metabolite")
                entry.symbol = id
                entry.description = name
                entry.addSynonym(name)
                entry.externalID = id
                entry.source = "HMDB"
                entry.organism = "HOMO SAPIENS"

                // Insert
                dictionaryLoader.insertEntry(entry)

            }
        } finally {
            dictionaryLoader.close()
        }
    }

}
