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
package fr.sanofi.fcl4transmart.controllers.listeners.description;

import java.io.File;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.workUI.description.ChangeNameUI;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
import fr.sanofi.fcl4transmart.ui.parts.StudySelectionPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

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
			this.changeNameUI.displayMessage("Error:\nThis name already exists");
			return;
		}
		oldPath.renameTo(newPath);
		this.study.setPath(newPath);
		this.study.setName(newName);
		StudySelectionPart.sendNameChanged(this.study);
		WorkPart.updateSteps();
	}

}
