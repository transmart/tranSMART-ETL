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

/** Serves as a data container for an entry that is to be added to a
 *  dictionary. Use it with DictionaryLoader to insert the symbol and synonyms
 *  into the appropriate database tables.
 */
class BioMarkerEntry {

    String id
    String symbol
    String description
    String organism
    String source
    String externalID
    String markerType
    String displayCategory
    List<String> synonyms = []

    public BioMarkerEntry(String markerType, String displayCategory) {
        this.markerType = markerType
        this.displayCategory = displayCategory
    }

    /** Adds the synonym if it doesn't exist in the synonyms or as symbol.
     */
    public void addSynonym(String synonym) {
        synonym = synonym.take(200)
        if (symbol != synonym && synonyms.find { it == synonym } == null) {
            synonyms.add(synonym)
        }
    }

    public void setSymbol(String value) {
        symbol = value.take(200)
    }

    public String setDescription(String value) {
        description = value.take(1000)
    }

}
