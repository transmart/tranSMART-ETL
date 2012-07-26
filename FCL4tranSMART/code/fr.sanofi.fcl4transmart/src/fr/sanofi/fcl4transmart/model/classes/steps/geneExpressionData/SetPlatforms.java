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
package fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData;

import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SetPlatformsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SetPlatforms implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetPlatforms(DataTypeItf dataType){
		this.workUI=new SetPlatformsUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set platforms";
	}
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This step allows defining platforms for samples.\n"+
				"The button 'Apply' allows setting all selected fields to the value in the field names 'Value'. All fields can be selected or deselected at the same time with buttons.\n"+
				"The button 'OK' allows updating the subject to sample mapping file.";
	}
	public boolean isAvailable(){
		try{
			if(((GeneExpressionData)this.dataType).getRawFile()==null){
				return false;
			}
			if(((GeneExpressionData)this.dataType).getStsmf()==null){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
