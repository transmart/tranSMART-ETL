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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetTermsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set replacing terms for clinical data
 */	
public class SetTerms implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetTerms(DataTypeItf dataType){
		this.workUI=new SetTermsUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set terms";
	}
	public String getDescription(){
		return "This step allows replacing terms of raw files by other terms.\n"+
				"A column of a raw file has to be chosen on the dropdown list. A field is then displayed for each term of this column. A new term can be indicated in this field\n"+
				"It is possible to map terms for a property with terms from another property, by selecting a property in the 'Copy mapping from column' list and click on the 'OK' button\n"+
				"By clicking on the 'OK' button, all the new terms of all columns are saved in a word mapping file";
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
