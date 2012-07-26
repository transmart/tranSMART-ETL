/*******************************************************************************
 * FC&L4tranSMART - Framework Curation And Loading For tranSMART
 * 
 * Copyright (c) 2012 Sanofi.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Sanofi - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers;

import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.DataTypeSelectionPart;

public class DataTypeSelectionController {
	private DataTypeSelectionPart dataTypeSelectionPart;
	public DataTypeSelectionController(DataTypeSelectionPart dataTypeSelectionPart){
		this.dataTypeSelectionPart=dataTypeSelectionPart;
	}
	public void selectionChanged(StudyItf selectedStudy){
		  this.dataTypeSelectionPart.setList(selectedStudy.getDataTypes());
	}
}
