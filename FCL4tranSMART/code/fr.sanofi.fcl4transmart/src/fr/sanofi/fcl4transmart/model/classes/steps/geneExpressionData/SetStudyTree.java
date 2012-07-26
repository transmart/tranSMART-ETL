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
package fr.sanofi.fcl4transmart.model.classes.steps.geneExpressionData;

import java.io.File;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SetStudyTreeUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SetStudyTree implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetStudyTree(DataTypeItf dataType){
		this.workUI=new SetStudyTreeUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set study tree";
	}
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This step allows defining the study ontology tree for gene expression data, from the study root."+
		"A node can be added by selecting the parent on the tree, filling the field 'New node' and clicking on 'Add node'.\n"+
		"A node or a label can be removed by selecting on the tree and clicking on the button 'Remove a node'.\n"+
		"A property can be added by selecting the parent on the tree, then the label on the dropdown list, and by clicking on the 'Add property' button\n"+
		"When the button 'OK' is clicked, the subject to sample mapping file is updated";
	}
	public boolean isAvailable(){
		try{
			if(((GeneExpressionData)this.dataType).getRawFile()==null){
				return false;
			}
			File stsmf=((GeneExpressionData)this.dataType).getStsmf();
			if(stsmf==null){
				return false;
			}
			if(!FileHandler.checkPlatform(stsmf)){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
