package org.transmartproject.pipeline.dictionary


import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Extracts protein dictionary and loads it into the database.
 */
class UniProtDictionary {
    private static final Logger log = Logger.getLogger(MiRBaseDictionary)

    static main(args) {
        if (!args) {
            println "UniProtDictionary <uniprot-dictionary.tsv>"
            System.exit(1)
        }

        def fileLocation = args[0]

        PropertyConfigurator.configure("conf/log4j.properties")
        File file = new File(fileLocation)
        UniProtDictionary dict = new UniProtDictionary()
        dict.loadData(file)
    }

    void loadData(File file) {
        if (!file.exists()) {
            log.error("File is not found: ${file.getAbsolutePath()}")
            return
        }

        DictionaryLoader dictionaryLoader = new DictionaryLoader();
        CorrelationLoader correlationLoader = new CorrelationLoader("PROTEIN TO GENE", "GENE TO PROTEIN");

        try {
            file.eachLine(0) { line, number ->
                if (number == 0) {
                    return // Skip the header line
                }

                // Extract data
                String[] split = line.split("\t")
                String uniProtNumber = split[0]
                String entryName = split[1] // symbol
                String proteinFullName = split[2].take(1000)
                String proteinName = split[3].take(200)
                String organism = split[4].toUpperCase()
                // for the preferred and alternative gene names, we'll have to
                // explicitly check for the split length, because groovy's split
                // ignores trailing fields if they're empty:
                String preferredGeneNames = ""
                if (split.size() > 5) {
                    preferredGeneNames = split[5]
                }
                String alternativeGeneNames = ""
                if (split.size() > 6) {
                    alternativeGeneNames = split[6]
                }
                List<String> genesToLink = []
                genesToLink.addAll(preferredGeneNames.split("; "))
                genesToLink.addAll(alternativeGeneNames.split(" "))

                // Insert biomarker (including search keywords and terms)
                BioMarkerEntry bioMarkerEntry = new BioMarkerEntry("PROTEIN", "Protein")
                bioMarkerEntry.symbol = entryName
                bioMarkerEntry.description = proteinFullName
                bioMarkerEntry.synonyms.add(uniProtNumber)
                bioMarkerEntry.synonyms.add(proteinName)
                bioMarkerEntry.externalID = uniProtNumber
                bioMarkerEntry.source = "UniProt"
                bioMarkerEntry.organism = organism
                dictionaryLoader.insertEntry(bioMarkerEntry)

                // Insert data correlation
                for (String geneSymbol: genesToLink) {
                    CorrelationEntry correlationEntry = new CorrelationEntry()
                    correlationEntry.symbol1 = entryName
                    correlationEntry.markerType1 = "PROTEIN"
                    correlationEntry.symbol2 = geneSymbol
                    correlationEntry.markerType2 = "GENE"
                    correlationEntry.organism = organism
                    correlationLoader.insertCorrelation(correlationEntry);
                }

            }
        }
        finally {
            dictionaryLoader.close()
            correlationLoader.close()
        }
    }

}
