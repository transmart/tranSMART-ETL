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


import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SelectRawFileUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SelectRawFile implements StepItf{
	private WorkItf workUI;
	public SelectRawFile(DataTypeItf dataType){
		this.workUI=new SelectRawFileUI(dataType);
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Select raw file";
	}
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This step allows chosing a raw file, by indicating the path or by choosing it with the 'browse' button.\n"+
				"When the button 'Add file' is clicked, the format of the file is checked, and then the file is copied in the workspace.";
	}
	public boolean isAvailable(){
		return true;
	}
}
