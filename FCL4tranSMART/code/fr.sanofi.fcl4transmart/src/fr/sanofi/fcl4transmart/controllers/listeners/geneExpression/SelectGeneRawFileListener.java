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
package fr.sanofi.fcl4transmart.controllers.listeners.geneExpression;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SelectRawFileUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class SelectGeneRawFileListener implements Listener{
	private SelectRawFileUI selectRawFileUI;
	private DataTypeItf dataType;
	public SelectGeneRawFileListener(SelectRawFileUI selectRawFileUI, DataTypeItf dataType){
		this.selectRawFileUI=selectRawFileUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String path=this.selectRawFileUI.getPath();
		if(path==null) return;
		File rawFile=new File(path);
		if(rawFile.exists()){
			if(rawFile.isFile()){
				if(!this.checkFormat(rawFile)) return;
				
				String newPath=this.dataType.getPath().getAbsolutePath()+File.separator+rawFile.getName();
				
				File copiedRawFile=new File(newPath);
				if(!copiedRawFile.exists()){
					try {
						FileUtils.copyFile(rawFile, copiedRawFile);
						((GeneExpressionData)this.dataType).setRawFile(copiedRawFile);
						
						this.selectRawFileUI.displayMessage("File has been added");
						WorkPart.updateSteps();
						//to do: update files list
						UsedFilesPart.sendFilesChanged(dataType);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else{
					this.selectRawFileUI.displayMessage("This file has already been added");
				}
			}
			else{
				this.selectRawFileUI.displayMessage("This is a directory");
			}
		}
		else{
			this.selectRawFileUI.displayMessage("This path does no exist");
		}
	}
	public boolean checkFormat(File file){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			line=br.readLine();
			//split must has a limit to take into account empty strings
			int columnsNbr=line.split("\t", 5000).length;
			if(columnsNbr<2){
				this.selectRawFileUI.displayMessage("Error:\nAt least two columns are required");
				br.close();
				return false;
			}
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] fields=line.split("\t",5000);
					if(fields.length!=columnsNbr){
						this.selectRawFileUI.displayMessage("Error:\nLines have no the same number of columns");
						br.close();
						return false;
					}
					for(int i=1; i<fields.length; i++){
						try{
							Double.valueOf(fields[i]);
						}
						catch(NumberFormatException e){
							this.selectRawFileUI.displayMessage("Error:\nIntensity values are to be numbers");
							br.close();
							return false;
						}
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
