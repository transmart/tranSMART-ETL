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

import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadAnnotationUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;
/**
 *This class represents the step allowing to check that a platform annotation has already been loaded
 */	
public class CheckAnnotation implements StepItf{
	private WorkItf workUI;
	public CheckAnnotation(DataTypeItf dataType){
		this.workUI=new LoadAnnotationUI(dataType);
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Check annotation";
	}
	public String getDescription(){
		return "This step allows checking that the Affymetrix platform used in the study has annotation already loaded in the database. The identifier of the platform (e.g. 'GPL6244') has to be indicated.\n"+
				"If annotation for this platform is not loaded yet, an annotation file can be chosen to be loaded. A title has to be provided, and optionally a date and a release number.\n"+
				"The file with platform annotation has to contain the following headers, in the right order:\n"+
				"\t\tGPL_ID  PROBE_ID  GENE_SYMBOL  GENE_ID  ORGANISM  \n"+
				"The gene identifier has to be a unique numeric identifier, and can be used to associate a pathway with a gene.\n"+
				"A database connection is needed for this step.\n"+
				"Warning: This step can be very long.";
	}
	public boolean isAvailable(){
		return true;
	}
}
