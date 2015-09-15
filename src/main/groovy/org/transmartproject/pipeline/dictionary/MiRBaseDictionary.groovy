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

/** Extracts miRNA data from file miRNA.dat (http://mirbase.org/ftp.shtml) and
 *  adds it to BIOMART.BIO_MARKER.
 */
class MiRBaseDictionary {
    private static final Logger log = Logger.getLogger(MiRBaseDictionary)

    static main(args) {
        if (!args) {
            println "MiRBaseDictionary <miRNA.dat> <aliases.txt>"
            System.exit(1)
        }

        def miRNAFileLocation = args[0]
        def aliasesFileLocation = args[1]

        PropertyConfigurator.configure("conf/log4j.properties")
        File miRNAFile = new File(miRNAFileLocation)
        File aliasesFile = new File(aliasesFileLocation)
        MiRBaseDictionary dict = new MiRBaseDictionary()
        dict.loadData(miRNAFile, aliasesFile)

        print new Date()
        println " MiRBASE dictionary load completed successfully"
    }

    void loadData(File miRNAFile, File aliasesFile) {
        if (!miRNAFile.exists()) {
            log.error("File is not found: ${miRNAFile.getAbsolutePath()}")
            return
        }
        if (!aliasesFile.exists()) {
            log.error("File is not found: ${aliasesFile.getAbsolutePath()}")
            return
        }

        // Parse input files
        List<BioMarkerEntry> entries = [];
        parseMiRBase(miRNAFile, entries);
        parseAliases(aliasesFile, entries);

        // Load into database
        DictionaryLoader dictionaryLoader = new DictionaryLoader();
        try {
            entries.each { dictionaryLoader.insertEntry(it) }
        } finally {
            dictionaryLoader.close()
        }

    }

    private void parseMiRBase(File miRNAFile, List<BioMarkerEntry> entries) {
        BioMarkerEntry miRBaseEntry = new BioMarkerEntry("MIRNA", "miRNA")

        miRNAFile.eachLine {
            if (it.startsWith("//")) {
                // Insert the current instance and start a new one
                if (miRBaseEntry.organism && miRBaseEntry.symbol) {
                    entries.add(miRBaseEntry)
                }
                miRBaseEntry = new BioMarkerEntry("MIRNA", "miRNA")
            } else if (it.startsWith("ID")) {
                // The id symbol (e.g. hsa-mir-100) will be a synonym and the external ID
                String[] split = it.substring(5).split(" ")
                String idSymbol = split[0]
                miRBaseEntry.externalID = idSymbol
                miRBaseEntry.addSynonym(idSymbol)

                // Extract the organism
                if (it.contains("; MMU; ")) {
                    miRBaseEntry.organism = "MUS MUSCULUS"
                }
                if (it.contains("; RNO; ")) {
                    miRBaseEntry.organism = "RATTUS NORVEGICUS"
                }
                if (it.contains("; HSA; ")) {
                    miRBaseEntry.organism = "HOMO SAPIENS"
                }
            } else if (it.startsWith("AC")) {
                miRBaseEntry.id = it[5..-2]
            } else if (it.startsWith("DE")) {
                miRBaseEntry.description = it.substring(5)
            } else if (it.startsWith("DR")) {
                //could try to extract aliases from here too?
                // could also look to /accession and /product values in FT blocks.
                if (it.contains("ENTREZGENE")) {
                    // Extract the Entrez gene code and use it as the symbol
                    // (There also seems to be an entrez gene id (str[1]),
                    // but we'll ignore it for now...)
                    String[] str = it.split("; ")
                    String code = str[-1]
                    if (code.endsWith('.')) {
                        code = code[0..-2]
                    }
                    miRBaseEntry.symbol = code.toUpperCase()
                    miRBaseEntry.source = 'Entrez'
                }
            }
        }
    }

    private void parseAliases(File aliasesFile, List<BioMarkerEntry> entries) {

        aliasesFile.eachLine {
            String[] split = it.split("\t")
            String id = split[0]
            BioMarkerEntry miRBaseEntry = entries.find { it.id == id }
            if (miRBaseEntry != null) {
                String[] aliases = split[1].split(";")
                aliases.each { miRBaseEntry.addSynonym(it) }
            }
        }

    }

}
