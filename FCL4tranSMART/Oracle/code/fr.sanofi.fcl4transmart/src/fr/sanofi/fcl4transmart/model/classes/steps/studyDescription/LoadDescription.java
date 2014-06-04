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

import fr.sanofi.fcl4transmart.model.classes.workUI.description.LoadDescriptionUI;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

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
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This step allows loading a study description in the database, with SQL commands.\n The study node has to be defined in the last step to display description into tranSMART.\n"+
				"For now, organisms can only be added if they have an entry in the database. Organisms in the database are presented in a dropdown.\n"+
				"When this step is chosen, if the description has already been loaded into the database, data are retrieved to be displayed. If they are modified and loaded again, data will be replaced.\n"+
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
