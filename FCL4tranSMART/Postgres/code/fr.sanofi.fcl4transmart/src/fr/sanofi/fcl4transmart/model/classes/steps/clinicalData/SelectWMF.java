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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectWMFUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to select the word mapping file
 */	
public class SelectWMF implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SelectWMF(DataTypeItf dataType){
		this.workUI=new SelectWMFUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Select Word Mapping File (optional)";
	}
	public String getDescription(){
		return "This step allows choosing a word mapping file, by indicating the path or by choosing it with the 'browse' button.\n"+
		"When he button 'Add file' is clicked, the format of the file is checked, and then the file is copied in the workspace with the extension '.word_mapping'";
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
