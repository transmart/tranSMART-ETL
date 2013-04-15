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
package fr.sanofi.fcl4transmart.model.classes.steps.studyDescription;

import fr.sanofi.fcl4transmart.model.classes.workUI.description.SetTopNodeUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to set the study top node
 */	
public class SetTopNode implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetTopNode(DataTypeItf dataType){
		this.dataType=dataType;
		this.workUI=new SetTopNodeUI(dataType);
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set study node";
	}
	public String getDescription(){
		return "This step allows defining the study node of the study, which is the place where the study will appear in tranSMART dataset explorer.\n"+
				"A tree can be built by adding free text nodes. Then the study can be added to the tree by indicating its name and click on the 'Add' button.\n"+
				"The study to load is indicated in orange. The other studies are indicated in grey.\n"+
				"This step requires a database connection.";
	}
	public boolean isAvailable(){
		if(this.dataType==null){
			return false;
		}
		if(this.dataType.getStudy().toString().compareTo("New_study")==0){
			return false;
		}
		return true;
	}
}
