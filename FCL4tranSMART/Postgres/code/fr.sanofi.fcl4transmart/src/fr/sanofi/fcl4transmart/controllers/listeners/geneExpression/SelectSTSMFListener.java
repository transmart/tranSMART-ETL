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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SelectSTSMFUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls subject to sample mapping file selection
 */	
public class SelectSTSMFListener implements Listener{
	private DataTypeItf dataType;
	private SelectSTSMFUI selectSTSMFUI;
	public SelectSTSMFListener(SelectSTSMFUI selectSTSMFUI, DataTypeItf dataType){
		this.selectSTSMFUI=selectSTSMFUI;
		this.dataType=dataType;		
	}
	@Override
	public void handleEvent(Event event) {
		String path=this.selectSTSMFUI.getPath();
		if(path==null) return;
		if(path.contains("%")){
			this.selectSTSMFUI.displayMessage("File name can not contain percent ('%') symbol.");
			return;
		}
		File file=new File(path);
		if(file.exists()){
			if(file.isFile()){
				if(!this.checkFormat(file)) return;
				String newPath;
				if(file.getName().endsWith(".subject_mapping")){
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName();
				}
				else{
					newPath=this.dataType.getPath().getAbsolutePath()+File.separator+file.getName()+".subject_mapping";
				}
				
				File copiedFile=new File(newPath);
				try {
					FileUtils.copyFile(file, copiedFile);
					((GeneExpressionData)this.dataType).setSTSMF(copiedFile);
					
					this.selectSTSMFUI.displayMessage("File has been loaded");
					WorkPart.updateSteps();
					//to do: update files list
					UsedFilesPart.sendFilesChanged(this.dataType);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					selectSTSMFUI.displayMessage("File error: "+e.getLocalizedMessage());
					e.printStackTrace();
				}
			}
			else{
				this.selectSTSMFUI.displayMessage("This is a directory");
			}
		}
		else{
			this.selectSTSMFUI.displayMessage("This path does no exist");
		}
	}
	/**
	 *Checks the subject to sample mapping file format
	 */	
	public boolean checkFormat(File file){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			Vector<String> samples=new Vector<String>();
			for(File rawFile: ((GeneExpressionData)this.dataType).getRawFiles()){
				samples.addAll(FileHandler.getSamplesId(rawFile));
			}
			Vector<String> samplesSTSMF=new Vector<String>();
			String category="";
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] fields=line.split("\t", -1);
					//check columns number
					if(fields.length!=9){
						this.selectSTSMFUI.displayMessage("Error:\nLines have not the right number of columns");
						br.close();
						return false;
					}
					//check that study id is set
					if(fields[0].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nStudy identifiers have to be set");
						br.close();
						return false;
					}
					//check that subject id is set
					if(fields[2].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nSubjects identifiers have to be set");
						br.close();
						return false;
					}	
					//check that samples id is set
					if(fields[3].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nSamples identifiers have to be set");
						br.close();
						return false;
					}	
					//check that platform is set
					if(fields[4].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nPlatform has to be set");
						br.close();
						return false;
					}
					//check that tissue type is set
					if(fields[5].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nTissue type has to be set");
						br.close();
						return false;
					}	
					//check that category codes are set
					if(fields[8].compareTo("")==0){
						this.selectSTSMFUI.displayMessage("Error:\nCategory codes have to be set");
						br.close();
						return false;
					}	
					if(category.compareTo("")==0){
						category=fields[8];
					}
					else{
						if(fields[8].compareTo(category)!=0){
							this.selectSTSMFUI.displayMessage("Category code has to be always the same");
							br.close();
							return false;
						}
					}
					if(!samplesSTSMF.contains(fields[3])){
						if(samples.contains(fields[3])){
							samplesSTSMF.add(fields[3]);
						}
					}
				}
			}
			if(samplesSTSMF.size()!=samples.size()){
				this.selectSTSMFUI.displayMessage("Error:\nSample identifiers are not the same than in raw data file");
				br.close();
				return false;
			}
			br.close();
		}catch (Exception e){
			selectSTSMFUI.displayMessage("Error: "+e.getLocalizedMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
}
