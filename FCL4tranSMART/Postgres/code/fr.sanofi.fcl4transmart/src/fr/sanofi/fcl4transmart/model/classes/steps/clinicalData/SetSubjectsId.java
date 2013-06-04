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

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetSubjectsIdUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set subject identifiers for clinical data
 */	
public class SetSubjectsId implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetSubjectsId(DataTypeItf dataType){
		this.workUI=new SetSubjectsIdUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set subject identifiers";
	}
	public String getDescription(){
		return "This step allows choosing the column corresponding to the subject identifier."+
				"If a column mapping file is already existing, the identifiers are retrieved in this file and given as parameters by default";
	}
	public boolean isAvailable(){
		try{
			if(((ClinicalData)this.dataType).getRawFiles().size()<1){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
