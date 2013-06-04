/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et Développement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et Développement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers;

import java.util.Vector;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.DataTypeSelectionPart;
/**
 * This class handles a data type selection
 */
public class DataTypeSelectionController {
	private DataTypeSelectionPart dataTypeSelectionPart;
	private Vector<DataTypeItf> otherDataTypes;//data types not associated with a study (e.g. file transfer)
	public DataTypeSelectionController(DataTypeSelectionPart dataTypeSelectionPart){
		this.dataTypeSelectionPart=dataTypeSelectionPart;
		this.otherDataTypes=new Vector<DataTypeItf>();
		
		//add data types not linked to studies
		
		this.dataTypeSelectionPart.setList(this.otherDataTypes);
	}
	/**
	 * Set the data types list to fit to the selected study
	 */
	public void selectionChanged(StudyItf selectedStudy){
		Vector<DataTypeItf> dataTypes=new Vector<DataTypeItf>();
		if(selectedStudy!=null) dataTypes.addAll(selectedStudy.getDataTypes());
		dataTypes.addAll(this.otherDataTypes);
		this.dataTypeSelectionPart.setList(dataTypes);
	}
}
