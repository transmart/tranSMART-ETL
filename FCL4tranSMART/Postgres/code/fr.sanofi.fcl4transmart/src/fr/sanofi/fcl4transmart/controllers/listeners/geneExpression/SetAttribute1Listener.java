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
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SetAttribute1UI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the setting of the first optional attribute of the sample to subject mapping file
 */	
public class SetAttribute1Listener implements Listener{
	private DataTypeItf dataType;
	private SetAttribute1UI setAttribute1UI;
	public SetAttribute1Listener(DataTypeItf dataType, SetAttribute1UI setAttribute1UI){
		this.dataType=dataType;
		this.setAttribute1UI=setAttribute1UI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		Vector<String> values=this.setAttribute1UI.getValues();
		Vector<String> samples=this.setAttribute1UI.getSamples();		
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".subject_mapping.tmp");
		File stsmf=((GeneExpressionData)this.dataType).getStsmf();
		if(stsmf==null){
			this.setAttribute1UI.displayMessage("Error: no subject to sample mapping file");
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
					String sample=fields[3];
					String attribute;
					if(samples.contains(sample)){
						attribute=values.get(samples.indexOf(sample));
					}
					else{
						br.close();
						return;
					}
					out.write(fields[0]+"\t"+fields[1]+"\t"+fields[2]+"\t"+sample+"\t"+fields[4]+"\t"+fields[5]+"\t"+attribute+"\t"+fields[7]+"\t"+fields[8]+"\n");
				}
				br.close();
			}catch (Exception e){
				setAttribute1UI.displayMessage("File error: "+e.getLocalizedMessage());
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
				this.setAttribute1UI.displayMessage("File error: "+ioe.getLocalizedMessage());
				return;
			}		
	  }catch (Exception e){
			this.setAttribute1UI.displayMessage("Error: "+e.getLocalizedMessage());
		  e.printStackTrace();
	  }
	this.setAttribute1UI.displayMessage("Subject to sample mapping file updated");
	WorkPart.updateSteps();
	WorkPart.updateFiles();
	}
}
