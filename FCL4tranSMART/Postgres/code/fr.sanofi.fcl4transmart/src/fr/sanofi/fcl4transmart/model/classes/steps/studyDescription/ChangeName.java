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

import fr.sanofi.fcl4transmart.model.classes.workUI.description.ChangeNameUI;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step to change a study name
 */	
public class ChangeName implements StepItf{
	private WorkItf workUI;
	public ChangeName(StudyItf study){
		this.workUI=new ChangeNameUI(study);
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Change identifier";
	}
	public String getDescription(){
		return "This step allows modifying the identifier of a study, which is a unique identifier used in database. It is specially used when a new study is created, and is named 'New_study' by default.\n"+
				"For now, it is not possible to modify a name of a study which has data already loaded in database, so a verification is done, and the fields is not editable if there is data for this study in the database.\n"+
				"A database connection is needed for this step.";
	}
	public boolean isAvailable(){
		return true;
	}
}
