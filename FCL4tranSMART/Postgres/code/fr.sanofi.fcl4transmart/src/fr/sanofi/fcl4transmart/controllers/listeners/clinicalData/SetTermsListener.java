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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetTermsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the terms setting step
 */	
public class SetTermsListener implements Listener{
	private SetTermsUI setTermsUI;
	private DataTypeItf dataType;
	public SetTermsListener(SetTermsUI setTermsUI, DataTypeItf dataType){
		this.setTermsUI=setTermsUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".words.tmp");
		try{			  
			FileWriter fw = new FileWriter(file);
			BufferedWriter out = new BufferedWriter(fw);
			out.write("Filename\tColumn Number\tOriginal Data Value\tNew Data Value\n");
			//  
			HashMap<String, Vector<String>> oldValues=this.setTermsUI.getOldValues();
			HashMap<String, Vector<String>> newValues=this.setTermsUI.getNewValues();
			
			for(String fullName: oldValues.keySet()){
				File rawFile=new File(this.dataType.getPath()+File.separator+fullName.split(" - ", -1)[0]);
				String header=fullName.split(" - ", -1)[1];
				int columnNumber=FileHandler.getHeaderNumber(rawFile, header);
				for(int i=0; i<oldValues.get(fullName).size(); i++){
					if(newValues.get(fullName).elementAt(i).compareTo("")!=0){
						out.write(rawFile.getName()+"\t"+columnNumber+"\t"+oldValues.get(fullName).elementAt(i)+"\t"+newValues.get(fullName).elementAt(i)+"\n");
					}
				}
			}
			
			out.close();
			try{
				File fileDest;
				if(((ClinicalData)this.dataType).getWMF()!=null){
					String fileName=((ClinicalData)this.dataType).getWMF().getName();
					((ClinicalData)this.dataType).getWMF().delete();
					fileDest=new File(this.dataType.getPath()+File.separator+fileName);
				}
				else{
					fileDest=new File(this.dataType.getPath()+File.separator+this.dataType.getStudy().toString()+".words");
				}			
				FileUtils.moveFile(file, fileDest);
				((ClinicalData)this.dataType).setWMF(fileDest);
			}
			catch(IOException ioe){
				this.setTermsUI.displayMessage("File error: "+ioe.getLocalizedMessage());
				return;
			}		
	  }catch (Exception e){
		  this.setTermsUI.displayMessage("Error: "+e.getLocalizedMessage());
		  e.printStackTrace();
	  }
	this.setTermsUI.displayMessage("Word mapping file updated");
	WorkPart.updateSteps();
	WorkPart.updateFiles();
	UsedFilesPart.sendFilesChanged(dataType);
	}
}
