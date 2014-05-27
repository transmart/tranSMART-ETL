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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetSubjectsIdUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the subject identifier step
 */	
public class SelectIdentifiersListener implements Listener{
	private SetSubjectsIdUI setSubjectsIdUI;
	private DataTypeItf dataType;
	public SelectIdentifiersListener(SetSubjectsIdUI setSubjectsIdUI, DataTypeItf dataType){
		this.setSubjectsIdUI=setSubjectsIdUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		//write in a new file
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns.tmp");
		try{
			 Vector<String> subjectIds=this.setSubjectsIdUI.getSubjectIds();
			 for(String s: subjectIds){
				 if(s.compareTo("")==0){
					 this.setSubjectsIdUI.displayMessage("Subjects identifier columns have to be choosen");
					 return;
				 }
			 }
			  
			  FileWriter fw = new FileWriter(file);
			  BufferedWriter out = new BufferedWriter(fw);
			  out.write("Filename\tCategory Code\tColumn Number\tData Label\tData Label Source\tControlled Vocab Code\n");
			  
			  //subject identifier
			  Vector<File> rawFiles=((ClinicalData)this.dataType).getRawFiles();
			  for(int i=0; i<rawFiles.size(); i++){
				  int columnNumber=FileHandler.getHeaderNumber(rawFiles.elementAt(i), subjectIds.elementAt(i));
				  if(columnNumber!=-1){
					  out.write(rawFiles.elementAt(i).getName()+"\t\t"+columnNumber+"\tSUBJ_ID\t\t\n");
				  }
			  }
				if(((ClinicalData)this.dataType).getCMF()==null){
					out.close();
					File fileDest=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns");
					FileUtils.moveFile(file, fileDest);
					((ClinicalData)this.dataType).setCMF(fileDest);		
					WorkPart.updateSteps();
				}
				else{
					try{
						BufferedReader br = new BufferedReader(new FileReader(((ClinicalData)this.dataType).getCMF()));
						String line=br.readLine();
						while ((line=br.readLine())!=null){
							String[] s=line.split("\t", -1);
							if(s[3].compareTo("SUBJ_ID")!=0){
								out.write(line+"\n");
							}
						}
						br.close();
					}catch (Exception e){
						this.setSubjectsIdUI.displayMessage("Error: "+e.getLocalizedMessage());
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
						this.setSubjectsIdUI.displayMessage("File error: "+ioe.getLocalizedMessage());
						return;
					}
					
				}
			  }catch (Exception e){
				  this.setSubjectsIdUI.displayMessage("Error: "+e.getLocalizedMessage());
				  e.printStackTrace();
			  }
			//this.setSubjectsIdUI.displayMessage("Column mapping file updated");
			this.checkSubjects();
			WorkPart.updateSteps();
			WorkPart.updateFiles();
			UsedFilesPart.sendFilesChanged(dataType);
	}
	private void checkSubjects(){
		Vector<File> rawFiles=((ClinicalData)this.dataType).getRawFiles();
		if(rawFiles.size()<2){
			this.setSubjectsIdUI.displayMessage("Column mapping file updated");
			return;
		}
		Vector<String> subjectIds=this.setSubjectsIdUI.getSubjectIds();
		Vector<String> lastId=null;
		Vector<String> id;
		for(int i=0; i<rawFiles.size(); i++){
			int columnNumber=FileHandler.getHeaderNumber(rawFiles.elementAt(i), subjectIds.elementAt(i));
			if(columnNumber!=-1){
				id=FileHandler.getTerms(rawFiles.get(i), subjectIds.elementAt(i));
				if(lastId!=null){
					if(id.size()!=lastId.size()){
						this.setSubjectsIdUI.displayMessage("Column Mapping file updated\n\nWarning: Subject identifiers are not the same for all raw data files");
						return;
					}
					for(String t: id){
						if(!lastId.contains(t)){
							this.setSubjectsIdUI.displayMessage("Column Mapping file updated\n\nWarning: Subject identifiers are not the same for all raw data files");
							return;
						}
					}
				}
				lastId=id;
			}
		}
		this.setSubjectsIdUI.displayMessage("Column mapping file updated");
	}
}
