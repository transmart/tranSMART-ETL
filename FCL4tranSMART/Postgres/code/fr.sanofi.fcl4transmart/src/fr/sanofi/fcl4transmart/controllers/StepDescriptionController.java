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

import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.ui.parts.StepDescriptionPart;
/**
 *This class controls the step description part
 */	
public class StepDescriptionController {
	private StepDescriptionPart stepDescriptionPart;
	public StepDescriptionController(StepDescriptionPart stepDescriptionPart){
		this.stepDescriptionPart=stepDescriptionPart;
	}
	/**
	 *Change the description if another step is selected
	 */	
	public void selectionChanged(StepItf selectedStep){
		  this.stepDescriptionPart.setDescription(selectedStep.getDescription());
	}
}
