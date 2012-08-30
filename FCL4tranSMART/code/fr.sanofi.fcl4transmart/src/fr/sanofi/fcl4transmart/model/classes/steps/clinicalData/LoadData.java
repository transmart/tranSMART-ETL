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
package fr.sanofi.fcl4transmart.model.classes.steps.clinicalData;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.LoadDataUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class LoadData implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public LoadData(DataTypeItf dataType){
		this.workUI=new LoadDataUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Load data";
	}
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This step allows loading clinical data from raw files and mapping files, using a Kettle job.\n"+
				"The place of the study in the dataset explorer tree has to be indicated in the 'Study description' data type. The tree is displayed, with the study to load in orange, to check that the study tree is well defined.\n"+
				"A database connection is needed for this step";
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
