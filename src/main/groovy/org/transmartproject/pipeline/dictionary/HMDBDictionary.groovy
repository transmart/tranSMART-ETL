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

        print new Date()
        println " HMDB dictionary completed successfully"
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
