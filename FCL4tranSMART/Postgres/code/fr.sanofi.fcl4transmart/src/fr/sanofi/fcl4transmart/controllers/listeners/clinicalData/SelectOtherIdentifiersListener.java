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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

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
import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetOtherIdsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the visit names and site identifiers selection step
 *Since version 1.2: also controls observation names 
 */	
public class SelectOtherIdentifiersListener implements Listener{
	private SetOtherIdsUI setOtherIdsUI;
	private DataTypeItf dataType;
	public SelectOtherIdentifiersListener(SetOtherIdsUI setOtherIdsUI, DataTypeItf dataType){
		this.setOtherIdsUI=setOtherIdsUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		 Vector<File> rawFiles=((ClinicalData)this.dataType).getRawFiles();
		 Vector<String> siteIds=this.setOtherIdsUI.getSiteIds();
		 Vector<String> visitNames=this.setOtherIdsUI.getVisitNames();
		 
		//write in a new file
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns.tmp");
		try{			  
			  FileWriter fw = new FileWriter(file);
			  BufferedWriter out = new BufferedWriter(fw);
			  out.write("Filename\tCategory Code\tColumn Number\tData Label\tData Label Source\tControlled Vocab Code\n");
			  
			  for(int i=0; i<rawFiles.size(); i++){
		  
				  //site identifier
				  if(siteIds.elementAt(i).compareTo("")!=0){
					  int columnNumber=FileHandler.getHeaderNumber(rawFiles.elementAt(i), siteIds.elementAt(i));
					  if(columnNumber!=-1){
						  out.write(rawFiles.elementAt(i).getName()+"\t\t"+columnNumber+"\tSITE_ID\t\t\n");
					  }
				  }
				  
				  //visit name
				  if(visitNames.elementAt(i).compareTo("")!=0){
					  int columnNumber=FileHandler.getHeaderNumber(rawFiles.elementAt(i), visitNames.elementAt(i));
					  if(columnNumber!=-1){
						  out.write(rawFiles.elementAt(i).getName()+"\t\t"+columnNumber+"\tVISIT_NAME\t\t\n");
					  }
				  }
			  }
			  //add lines from existing CMF
				try{
					BufferedReader br = new BufferedReader(new FileReader(((ClinicalData)this.dataType).getCMF()));
					String line=br.readLine();
					while ((line=br.readLine())!=null){
						String[] s=line.split("\t", -1);
						if(s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("VISIT_NAME")!=0){
							out.write(line+"\n");
						}
					}
					br.close();
				}catch (Exception e){
					this.setOtherIdsUI.displayMessage("Error: "+e.getLocalizedMessage());
					e.printStackTrace();
					out.close();
				}
				out.close();
				try{
					String fileName=((ClinicalData)this.dataType).getCMF().getName();
					((ClinicalData)this.dataType).getCMF().delete();
					File fileDest=new File(this.dataType.getPath()+File.separator+fileName);
					FileUtils.moveFile(file, fileDest);
					((ClinicalData)this.dataType).setCMF(fileDest);
				}
				catch(IOException ioe){
					this.setOtherIdsUI.displayMessage("File error: "+ioe.getLocalizedMessage());
					return;
				}
				
				
			  }catch (Exception e){
				  this.setOtherIdsUI.displayMessage("Error: "+e.getLocalizedMessage());
				  e.printStackTrace();
			  }
			this.setOtherIdsUI.displayMessage("Column mapping file updated");
			WorkPart.updateSteps();
			WorkPart.updateFiles();
			UsedFilesPart.sendFilesChanged(dataType);
	}
}
