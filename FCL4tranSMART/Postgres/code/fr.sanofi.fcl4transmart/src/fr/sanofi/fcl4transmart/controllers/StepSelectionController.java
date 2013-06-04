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

import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.StepSelectionPart;
/**
 *This class controls the step selection
 */	
public class StepSelectionController {
	private StepSelectionPart stepSelectionPart;
	public StepSelectionController(StepSelectionPart stepSelectionPart){
		this.stepSelectionPart=stepSelectionPart;
	}
	/**
	 *Changes the step list if a new data type is selected
	 */	
	public void selectionChanged(DataTypeItf selectedDataType){
		  this.stepSelectionPart.setList(selectedDataType.getSteps());
	  }
}
