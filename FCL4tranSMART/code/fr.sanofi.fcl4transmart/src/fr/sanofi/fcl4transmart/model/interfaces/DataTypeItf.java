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
package fr.sanofi.fcl4transmart.model.interfaces;

import java.io.File;
import java.util.Vector;

public interface DataTypeItf {
	public Vector<StepItf> getSteps();
	public void setFiles(File path);
	public Vector<File> getFiles();
	public StudyItf getStudy();
	public File getPath();
}
