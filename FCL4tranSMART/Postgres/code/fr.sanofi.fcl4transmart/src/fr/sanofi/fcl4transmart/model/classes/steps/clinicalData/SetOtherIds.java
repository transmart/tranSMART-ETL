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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetOtherIdsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set the visit names, sub-visit names and site identifiers columns for clinical data
 */	
public class SetOtherIds implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetOtherIds(DataTypeItf dataType){
		this.workUI=new SetOtherIdsUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set other identifiers (optional)";
	}
	public String getDescription(){
		return "This optional step allows choosing the column corresponding to site, visit and sub-visit identifiers.\n"+
				"A sub-visit name column can not be set if visit name column is not.\n"+
				"If a column mapping file is already existing, the identifiers are retrieved in this file and given as parameters by default";
	}
	public boolean isAvailable(){
		try{
			if(((ClinicalData)this.dataType).getRawFiles().size()<1){
				return false;
			}
			if(((ClinicalData)this.dataType).getCMF()==null){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
