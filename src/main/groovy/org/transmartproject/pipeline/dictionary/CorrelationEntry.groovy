package org.transmartproject.pipeline.dictionary

/** Serves as a data container for an entry that is to be added as a
 *  correlation/pathway. Use it with CorrelationLoader to insert the data
 *  into the appropriate database tables.
 */
class CorrelationEntry {

    String symbol1
    String symbol2
    String markerType1
    String markerType2
    String correlation //Name? or use id?
    String organism

    public CorrelationEntry() {
    }

}
