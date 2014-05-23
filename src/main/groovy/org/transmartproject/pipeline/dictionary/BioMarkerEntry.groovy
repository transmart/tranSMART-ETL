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
