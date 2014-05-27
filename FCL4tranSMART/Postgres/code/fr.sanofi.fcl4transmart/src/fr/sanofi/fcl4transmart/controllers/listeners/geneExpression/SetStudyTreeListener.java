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
package fr.sanofi.fcl4transmart.controllers.listeners.geneExpression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.TreeNode;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SetStudyTreeUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the setting of the category code attribute of the sample to subject mapping file
 */	
public class SetStudyTreeListener implements Listener{
	private DataTypeItf dataType;
	private SetStudyTreeUI setStudyTreeUI;
	public SetStudyTreeListener(SetStudyTreeUI setStudyTreeUI, DataTypeItf dataType){
		this.dataType=dataType;
		this.setStudyTreeUI=setStudyTreeUI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".subject_mapping.tmp");
		File stsmf=((GeneExpressionData)this.dataType).getStsmf();
		if(stsmf==null){
			this.setStudyTreeUI.displayMessage("Error: no subject to sample mapping file");
		}
		String category="";
		TreeNode node=this.setStudyTreeUI.getRoot();
		if(!node.hasChildren()){
			this.setStudyTreeUI.displayMessage("You have to set a category code");
			return;
		}
		node=node.getChildren().get(0);
		while(node!=null){
			if(category.compareTo("")==0){
				category+=node.toString().replace(' ', '_');
			}
			else{
				category+="+"+node.toString().replace(' ', '_');
			}
			if(node.hasChildren()){
				node=node.getChildren().get(0);
			}
			else{
				node=null;
			}
		}
		try{			  
			FileWriter fw = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fw);
			out.write("study_id\tsite_id\tsubject_id\tSAMPLE_ID\tPLATFORM\tTISSUETYPE\tATTR1\tATTR2\tcategory_cd\n");
			
			try{
				BufferedReader br = new BufferedReader(new FileReader(stsmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] fields=line.split("\t", -1);
					out.write(fields[0]+"\t"+fields[1]+"\t"+fields[2]+"\t"+fields[3]+"\t"+fields[4]+"\t"+fields[5]+"\t"+fields[6]+"\t"+fields[7]+"\t"+category+"\n");
				}
				br.close();
			}catch (Exception e){
				this.setStudyTreeUI.displayMessage("File error: "+e.getLocalizedMessage());
				out.close();
				e.printStackTrace();
			}	
			out.close();
			try{
				File fileDest;
				if(stsmf!=null){
					String fileName=stsmf.getName();
					stsmf.delete();
					fileDest=new File(this.dataType.getPath()+File.separator+fileName);
				}
				else{
					fileDest=new File(this.dataType.getPath()+File.separator+this.dataType.getStudy().toString()+".subject_mapping");
				}			
				FileUtils.moveFile(file, fileDest);
				((GeneExpressionData)this.dataType).setSTSMF(fileDest);
			}
			catch(IOException ioe){
				this.setStudyTreeUI.displayMessage("File error: "+ioe.getLocalizedMessage());
				return;
			}		
	  }catch (Exception e){
		  this.setStudyTreeUI.displayMessage("Eerror: "+e.getLocalizedMessage());
		  e.printStackTrace();
	  }
	this.setStudyTreeUI.displayMessage("Subject to sample mapping file updated");
	WorkPart.updateSteps();
	WorkPart.updateFiles();
	}

}
