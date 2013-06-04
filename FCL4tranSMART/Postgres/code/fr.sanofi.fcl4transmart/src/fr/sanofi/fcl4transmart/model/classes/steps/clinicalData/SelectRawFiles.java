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

import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectRawFilesUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to select raw files for clinical data
 */	
public class SelectRawFiles implements StepItf{
	private WorkItf workUI;
	public SelectRawFiles(DataTypeItf dataType){
		this.workUI=new SelectRawFilesUI(dataType);
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Select raw files";
	}
	public String getDescription(){
		return "This step allows adding one or several raw files, by indicating the paths, separated with a ';' character, or by choosing them with the 'browse' button.\n"+
				"The format of the file, tab delimited or soft(GEO), has to be indicated in the 'Format' dropdown.\n"+
				"When the button 'Add files' is clicked, the format of the files is checked, and then the files are:\n"+
				"-For a tab delimited file: copied in the workspace\n"+
				"-For a soft file: information are get from the \"^SAMPLE = value\" line, and from the \"!Sample_characteristics_ch1 = property: value\" lines (under the form of a property/value couple), then a tab delimited file is created in the workspace with this information\n\n"+
				"It is also possible to remove one or several files by selecting them in the corresponding list and then click on the 'Remove selected files' button";
	}
	public boolean isAvailable(){
		return true;
	}
}
