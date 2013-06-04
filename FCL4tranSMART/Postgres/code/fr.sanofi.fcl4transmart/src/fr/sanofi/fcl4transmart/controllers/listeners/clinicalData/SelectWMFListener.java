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
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectWMFUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the word mapping file selection
 */	
public class SelectWMFListener implements Listener{
	private DataTypeItf dataType;
	private SelectWMFUI selectWMFUI;
	public SelectWMFListener(SelectWMFUI selectWMFUI, DataTypeItf dataType){
		this.selectWMFUI=selectWMFUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String path=this.selectWMFUI.getPath();
		if(path==null) return;
		if(path.contains("%")){
			this.selectWMFUI.displayMessage("File name can not contain percent ('%') symbol.");
			return;
		}
		File file=new File(path);
		if(file.exists()){
			if(file.isFile()){
				if(!this.checkFormat(file)) return;
				
				String newPath;
				if(file.getName().endsWith(".words")){
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName();
				}
				else{
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName()+".words";
				}

				File copiedFile=new File(newPath);
				try {
					FileUtils.copyFile(file, copiedFile);
					((ClinicalData)this.dataType).setWMF(copiedFile);
					
					this.selectWMFUI.displayMessage("File has been added");
					WorkPart.updateSteps();
					//to do: update files list
					UsedFilesPart.sendFilesChanged(dataType);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					this.selectWMFUI.displayMessage("Error: "+e.getLocalizedMessage());
					e.printStackTrace();
				}
			}
			else{
				this.selectWMFUI.displayMessage("This is a directory");
			}
		}
		else{
			this.selectWMFUI.displayMessage("This path does no exist");
		}
	}
	/**
	 *Checks the format of a word mapping file
	 */	
	public boolean checkFormat(File file){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] fields=line.split("\t", -1);
					//check columns number
					if(fields.length!=4){
						this.selectWMFUI.displayMessage("Error:\nLines have not the right number of columns");
						br.close();
						return false;
					}
					//check raw file names
					if(!((ClinicalData)this.dataType).getRawFilesNames().contains(fields[0])){
						this.selectWMFUI.displayMessage("Error:\ndata file '"+fields[0]+"' does not exist");
						br.close();
						return false;
					}
					//check that column number is set
					if(fields[1].compareTo("")==0){
						this.selectWMFUI.displayMessage("Error:\nColumns numbers have to be set");
						br.close();
						return false;
					}
					try{
						Integer.parseInt(fields[1]);
					}
					catch(NumberFormatException e){
						this.selectWMFUI.displayMessage("Error:\nColumns numbers have to be numbers");
						br.close();
						return false;
					}
					//check that original data value is set
					if(fields[2].compareTo("")==0){
						this.selectWMFUI.displayMessage("Error:\nOriginal data values have to be set");
						br.close();
						return false;
					}
					//check that new data value is set
					if(fields[3].compareTo("")==0){
						this.selectWMFUI.displayMessage("Error:\nNew data values have to be set");
						br.close();
						return false;
					}	
				}
			}
			br.close();
		}catch (Exception e){
			this.selectWMFUI.displayMessage("Error: "+e.getLocalizedMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}

}
