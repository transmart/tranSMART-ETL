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

import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.StepSelectionPart;

public class StepSelectionController {
	private StepSelectionPart stepSelectionPart;
	public StepSelectionController(StepSelectionPart stepSelectionPart){
		this.stepSelectionPart=stepSelectionPart;

	}
	public void selectionChanged(DataTypeItf selectedDataType){
		  this.stepSelectionPart.setList(selectedDataType.getSteps());
	  }
}
