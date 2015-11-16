/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2012-2014 The TranSMART Foundation
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version, along with the following terms:
 *
 * 1.	You may convey a work based on this program in accordance with section 5,
 *      provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it,
 *      in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************/
  
package org.transmartproject.pipeline.dictionary


import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator

/** Extracts protein dictionary and loads it into the database.
 */
class UniProtDictionary {
    private static final Logger log = Logger.getLogger(UniProtDictionary)

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

        print new Date()
        println " UniProt dictionary load completed successfully"
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
                String secondaryAccessions = ""
                if (split.size() > 7) {
                    secondaryAccessions = split[7]
                }
                List<String> genesToLink = []
                if(preferredGeneNames != "") {
                    genesToLink.addAll(preferredGeneNames.split("; "))
                }
                if(alternativeGeneNames != "") {
                    genesToLink.addAll(alternativeGeneNames.split(" "))
                }
                
                List<String> accnumToLink = []
                if(secondaryAccessions != "") {
                    accnumToLink.addAll(secondaryAccessions.split(";"))
                }

                // Insert biomarker (including search keywords and terms)
                BioMarkerEntry bioMarkerEntry = new BioMarkerEntry("PROTEIN", "Protein")
                bioMarkerEntry.symbol = entryName
                bioMarkerEntry.description = proteinFullName
                bioMarkerEntry.synonyms.add(uniProtNumber)
                bioMarkerEntry.synonyms.add(proteinName)
                bioMarkerEntry.externalID = uniProtNumber
                bioMarkerEntry.source = "UniProt"
                bioMarkerEntry.organism = organism
                for (String secAccnum: accnumToLink) {
                    bioMarkerEntry.synonyms.add(secAccnum)
                }

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
