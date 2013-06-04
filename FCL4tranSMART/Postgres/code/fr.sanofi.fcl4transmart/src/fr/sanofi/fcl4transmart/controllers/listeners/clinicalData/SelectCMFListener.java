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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectCMFUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

import org.apache.commons.io.FileUtils;
/**
 *This class controls a column mapping file selection
 */	
public class SelectCMFListener implements Listener{
	private SelectCMFUI selectCMFUI;
	private DataTypeItf dataType;
	public SelectCMFListener(SelectCMFUI selectCMFUI, DataTypeItf dataType){
		this.selectCMFUI=selectCMFUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String path=this.selectCMFUI.getPath();
		if(path==null) return;
		if(path.contains("%")){
			this.selectCMFUI.displayMessage("File name can not contain percent ('%') symbol.");
			return;
		}
		File file=new File(path);
		if(file.exists()){
			if(file.isFile()){
				if(!this.checkFormat(file)) return;
				String  newPath;
				if(file.getName().endsWith(".columns")){
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName();
				}
				else{
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName()+".columns";
				}

				File copiedFile=new File(newPath);
				try {
					FileUtils.copyFile(file, copiedFile);
					((ClinicalData)this.dataType).setCMF(copiedFile);
					
					this.selectCMFUI.displayMessage("File has been added");
					WorkPart.updateSteps();
					//to do: update files list
					UsedFilesPart.sendFilesChanged(dataType);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					this.selectCMFUI.displayMessage("File error: "+e.getLocalizedMessage());
					e.printStackTrace();
				}
			}
			else{
				this.selectCMFUI.displayMessage("This is a directory");
			}
		}
		else{
			this.selectCMFUI.displayMessage("This path does no exist");
		}
	}
	/**
	 *Checks the format of a column mapping file
	 */	
	public boolean checkFormat(File file){
		boolean isSubjectIdSet=false;
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] fields=line.split("\t", -1);
					//check columns number
					if(fields.length!=6){
						this.selectCMFUI.displayMessage("Error:\nLines have not the right number of columns");
						br.close();
						return false;
					}
					//check raw file names
					if(!((ClinicalData)this.dataType).getRawFilesNames().contains(fields[0])){
						this.selectCMFUI.displayMessage("Error:\nData file '"+fields[0]+"' does not exist");
						br.close();
						return false;
					}
					//check that subject identifier is set
					if(fields[3].compareTo("SUBJ_ID")==0){
						isSubjectIdSet=true;
					}
					//check that column number is set and is a number
					if(fields[2].compareTo("")==0){
						this.selectCMFUI.displayMessage("Error:\nColumns numbers have to be set");
						br.close();
						return false;
					}
					try{
						Integer.parseInt(fields[2]);
					}
					catch(NumberFormatException e){
						this.selectCMFUI.displayMessage("Error:\nColumns numbers have to be numbers");
						br.close();
						return false;
					}
					//check that datalabel is set
					if(fields[3].compareTo("")==0){
						this.selectCMFUI.displayMessage("Error:\nData labels have to be set");
						br.close();
						return false;
					}
					//check that category code is set, except for reserved words
					if(!(fields[3].compareTo("SUBJ_ID")==0 || fields[3].compareTo("OMIT")==0 || fields[3].compareTo("SITE_ID")==0 || fields[3].compareTo("VISIT_NAME")==0) && fields[1].compareTo("")==0){
						this.selectCMFUI.displayMessage("Error:\nCategory codes have to be set");
						br.close();
						return false;
					}
					//check that data label source is set if the data label is '\'
					if(fields[3].compareTo("\\")==0 && fields[4].compareTo("")==0){
						this.selectCMFUI.displayMessage("Error:\nData label sources have to be set");
						br.close();
						return false;
					}
				}
			}
			br.close();
		}catch (Exception e){
			this.selectCMFUI.displayMessage("File error: "+e.getLocalizedMessage());
			e.printStackTrace();
			return false;
		}
		if(!isSubjectIdSet){
			this.selectCMFUI.displayMessage("Error:\nSubject identifiers have to be set");
			return false;
		}
		return true;
	}
}
