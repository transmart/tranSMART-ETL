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
package fr.sanofi.fcl4transmart.model.interfaces;

import java.io.File;
import java.util.Vector;
/**
 *This interface offers methods for a class representing a data type
 */
public interface DataTypeItf {
	/**
	 *Returns a vector containing objects implementing the interface StepItf representing a step, allowing to load this data type
	 *These steps have to be in the right order to load data
	 */
	public Vector<StepItf> getSteps();
	/**
	 *Sets the folder path for this data type, and find file associated with a data type
	 */
	public void setFiles(File path);
	/**
	 *Returns a vector of files for display in the file viewer list 
	 */
	public Vector<File> getFiles();
	/**
	 *Returns the study associated with an instance of a data type object
	 */
	public StudyItf getStudy();
	public File getPath();
}
