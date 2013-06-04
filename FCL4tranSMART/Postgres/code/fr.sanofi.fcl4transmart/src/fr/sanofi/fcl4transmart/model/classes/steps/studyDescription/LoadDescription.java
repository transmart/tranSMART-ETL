/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et D�veloppement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et D�veloppement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.model.classes.steps.studyDescription;

import fr.sanofi.fcl4transmart.model.classes.workUI.description.LoadDescriptionUI;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to load a study description
 */	
public class LoadDescription implements StepItf{
	private WorkItf workUI;
	private StudyItf study;
	public LoadDescription(StudyItf study){
		this.workUI=new LoadDescriptionUI(study);
		this.study=study;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Load description";
	}
	public String getDescription(){
		return "This step allows loading a study description in the database.\n The study node has to be defined in the last step to display description into tranSMART.\n"+
				"Study description is loaded as pairs of key/value. A pair can be added by clicking on the button 'Add a tag'. A pair can be removed by clicking on the button 'Remove tag' corresponding to the wanted line.\n"+
				"When this step is chosen, if the description has already been loaded into the database, data is retrieved to be displayed. If they are modified and loaded again, data will be replaced.\n"+
				"A database connection is needed for this step.";
	}
	public boolean isAvailable(){
		if(study==null){
			return false;
		}
		if(this.study.toString().compareTo("New_study")==0){
			return false;
		}
		return true;
	}
}
