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

import org.eclipse.swt.widgets.Composite;

import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *Controls the work part
 */	
public class WorkPartController {
	private WorkPart workPart;
	public WorkPartController(WorkPart workPart){
		this.workPart=workPart;
	}
	/**
	 *Changes the work part to fit to the selected step
	 */	
	public void selectionChanged(StepItf selectedStep, Composite parent){
		  Composite composite=selectedStep.getWorkUI().createUI(parent);
		  this.workPart.changeChild(composite);
	}
}
