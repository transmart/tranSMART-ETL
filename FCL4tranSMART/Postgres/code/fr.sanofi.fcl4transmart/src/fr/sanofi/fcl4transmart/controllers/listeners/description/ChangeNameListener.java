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
package fr.sanofi.fcl4transmart.controllers.listeners.description;

import java.io.File;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.workUI.description.ChangeNameUI;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.StudySelectionPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the study identifier modification
 */	
public class ChangeNameListener implements Listener{
	private ChangeNameUI changeNameUI;
	private StudyItf study;
	public ChangeNameListener(ChangeNameUI changeNameUI, StudyItf study){
		this.changeNameUI=changeNameUI;
		this.study=study;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String newName=this.changeNameUI.getName();
		File oldPath=this.study.getPath();
		File newPath=new File(oldPath.getParentFile().getAbsoluteFile()+File.separator+newName);
		if(newPath.exists()){
			this.changeNameUI.displayMessage("Error:\nThis identifier already exists");
			return;
		}
		String name=newPath.getName();
		
		//characters not allowed in folder name: " * / : < > ? \ | 
		if(name.contains("'") || name.contains(" ") || name.contains("\"") || name.contains("*") || name.contains("/") || name.contains(":") || name.contains("<") || name.contains(">") || name.contains("?") || name.contains("\\") || name.contains("|")){
			this.changeNameUI.displayMessage("The following characters are forbidden: ', \", *, /, :, <, >, ?, \\, |");
			return;
		}
		if(name.length()>25){
			this.changeNameUI.displayMessage("The maximum length for a study identifier is 25 characters");
			return;
		}
		oldPath.renameTo(newPath);
		this.study.setPath(newPath);
		this.study.setName(newName);
		StudySelectionPart.sendNameChanged(this.study);
		this.changeNameUI.displayMessage("The identifier has been changed");
		WorkPart.updateSteps();
	}

}
