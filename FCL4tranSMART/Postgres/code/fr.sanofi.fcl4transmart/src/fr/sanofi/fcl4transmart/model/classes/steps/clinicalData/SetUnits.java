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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetUnitsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set units for properties
 */	
public class SetUnits implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetUnits(DataTypeItf dataType){
		this.workUI=new SetUnitsUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set units";
	}
	public String getDescription(){
		return "This step allows setting units for properties.\n"+
				"The value column list has to be set to the column for which a unit will apply, and the value column has to be set to the column containing the units.\n"+
				"The 'add a line' button allows to set several units for several properties. If there is more than two lines, the 'Remove line' button present for each line except the first allows removing a line.\n"+
				"If a line is empty (except for the first), it has to be removed to update the column mapping file.";
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
