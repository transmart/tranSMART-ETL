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
package fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData;

import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SelectSTSMFUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to select a subject to sample mapping file
 */	
public class SelectSTSMF implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SelectSTSMF(DataTypeItf dataType){
		this.workUI=new SelectSTSMFUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Select Subject To Sample Mapping File";
	}

	public String getDescription(){
		return "This step allows choosing the subject to sample mapping file, by indicating the path or by choosing it with the 'browse' button.\n"+
				"When the button 'Add file' is clicked, the format of the file is checked, and then the file is copied in the workspace with the extension '.subject_mapping'\n."+
				"Warning: the subject to sample mapping file has to contain a header line, the first line is not considered as data, and so is not read.\n"+
				"The columns of the subject to sample are the following: study identifier, site identifier, subject identifier, sample identifier, platform, tissue type, attribute 1, attribute 2, category code";
	}
	public boolean isAvailable(){
		try{
			if(((GeneExpressionData)this.dataType).getRawFiles()==null || ((GeneExpressionData)this.dataType).getRawFiles().size()==0){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
