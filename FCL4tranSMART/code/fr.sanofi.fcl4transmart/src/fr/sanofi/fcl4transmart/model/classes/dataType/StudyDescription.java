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
package fr.sanofi.fcl4transmart.model.classes.dataType;

import java.io.File;
import java.util.Vector;
import fr.sanofi.fcl4transmart.model.classes.steps.studyDescription.ChangeName;
import fr.sanofi.fcl4transmart.model.classes.steps.studyDescription.LoadDescription;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;

public class StudyDescription implements DataTypeItf{
	private Vector<StepItf> steps;
	private StudyItf study;
	private File path;
	public StudyDescription(StudyItf study){
		this.study=study;
		this.steps=new Vector<StepItf>();
		
		//add the different steps here
		this.steps.add(new ChangeName(study));
		this.steps.add(new LoadDescription(study));
	}
	@Override
	public Vector<StepItf> getSteps() {
		return this.steps;
	}
	public String toString(){
		return "Study description";
	}
	public void setFiles(File path){
		//
	}
	public Vector<File> getFiles(){
		return null;
	}
	public StudyItf getStudy(){
		return this.study;
	}
	public File getPath(){
		return this.path;
	}
}
