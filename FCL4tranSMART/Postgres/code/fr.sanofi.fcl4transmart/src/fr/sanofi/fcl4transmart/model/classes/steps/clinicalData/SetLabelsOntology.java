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
package fr.sanofi.fcl4transmart.model.classes.steps.clinicalData;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetLabelsOntologyUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set labels ontology (term and code) for clinical data
 */	
public class SetLabelsOntology implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetLabelsOntology(DataTypeItf dataType){
		this.workUI=new SetLabelsOntologyUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set labels ontology";
	}
	public String getDescription(){
		return "This step allows choosing labels for the study parameters, and optionaly mapping these labels with a controlled vocabulary code.\n"+
				"These controlled vocabulary codes can be find with bioontology portal:\n http://bioportal.bioontology.org/\n"+
				"Only labels which have been put in the data tree are presented.";
	}
	public boolean isAvailable(){
		try{
			if(((ClinicalData)this.dataType).getRawFiles().size()<1){
				return false;
			}
			if(((ClinicalData)this.dataType).getCMF()==null){
				return false;
			}
			if(!FileHandler.checkTreeSet(((ClinicalData)this.dataType).getCMF())){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
