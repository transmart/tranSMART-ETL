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
package fr.sanofi.fcl4transmart.controllers.listeners.clinicalData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectRawFilesUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class RemoveRawFileListener implements Listener{
	private SelectRawFilesUI selectRawFilesUI;
	private DataTypeItf dataType;
	public RemoveRawFileListener(DataTypeItf dataType, SelectRawFilesUI selectRawFilesUI){
		this.dataType=dataType;
		this.selectRawFilesUI=selectRawFilesUI;
	}
	@Override
	public void handleEvent(Event event) {
		File file=this.selectRawFilesUI.getSelectedRemovedFile();
		File cmf=((ClinicalData)this.dataType).getCMF();
		File wmf=((ClinicalData)this.dataType).getWMF();
		if(file==null){
			return;
		}
		if(((ClinicalData)this.dataType).getRawFiles().size()==1){
			if(cmf!=null || wmf!=null){
				if(this.selectRawFilesUI.confirm("The column mapping file  and the word mapping file will be removed.\nAre you sure to remove this file?")){
					if(cmf!=null){
						((ClinicalData)this.dataType).setCMF(null);
						try {
							FileUtils.forceDelete(cmf);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if(wmf!=null){
						((ClinicalData)this.dataType).setWMF(null);
						try {
							FileUtils.forceDelete(wmf);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			else{
				if(this.selectRawFilesUI.confirm("Are you sure to remove this file?")){
					((ClinicalData)this.dataType).getRawFiles().remove(file);
					FileUtils.deleteQuietly(file);
					UsedFilesPart.sendFilesChanged(dataType);
				}
			}
		}
		else{//several raw files: update cmf and wmf to remove lines for this raw file
			if(cmf!=null || wmf!=null){
				if(this.selectRawFilesUI.confirm("The column mapping file  and the word mapping file lines for this raw file will be deleted.\nAre you sure to remove this file?")){
					File newCmf=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns.tmp");
					try{			  
						  FileWriter fw = new FileWriter(newCmf);
						  BufferedWriter out = new BufferedWriter(fw);
							try{
								BufferedReader br=new BufferedReader(new FileReader(cmf));
								String line;
								while ((line=br.readLine())!=null){
									if(line.split("\t", 40)[0].compareTo(file.getName())!=0){
										out.write(line+"\n");
									}
								}
								br.close();
							}catch (Exception e){
								e.printStackTrace();
								out.close();
							}
							out.close();
							String fileName=cmf.getName();
							FileUtils.deleteQuietly(cmf);
							try{
								File fileDest=new File(this.dataType.getPath()+File.separator+fileName);
								FileUtils.moveFile(newCmf, fileDest);
								((ClinicalData)this.dataType).setCMF(fileDest);
							}
							catch(Exception ioe){
								this.selectRawFilesUI.displayMessage("File error");
								return;
							}
					  }catch (Exception e){
						  e.printStackTrace();
					  }
					File newWmf=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".words.tmp");
					try{			  
						  FileWriter fw = new FileWriter(newWmf);
						  BufferedWriter out = new BufferedWriter(fw);
						  try{
								BufferedReader br=new BufferedReader(new FileReader(wmf));
								String line;
								while ((line=br.readLine())!=null){
									if(line.split("\t", 40)[0].compareTo(file.getName())!=0){
										out.write(line+"\n");
									}
								}
								br.close();
							}catch (Exception e){
								e.printStackTrace();
								out.close();
							}
							out.close();
							String fileName=wmf.getName();
							FileUtils.deleteQuietly(wmf);
							try{
								File fileDest=new File(this.dataType.getPath()+File.separator+fileName);
								FileUtils.moveFile(newWmf, fileDest);
								((ClinicalData)this.dataType).setWMF(fileDest);
							}
							catch(Exception ioe){
								this.selectRawFilesUI.displayMessage("File error");
								return;
							}
					  }catch (Exception e){
						  e.printStackTrace();
					  }
					((ClinicalData)this.dataType).getRawFiles().remove(file);
					FileUtils.deleteQuietly(file);
				}
			}
			else{
				if(this.selectRawFilesUI.confirm("Are you sure to remove this file?")){
					((ClinicalData)this.dataType).getRawFiles().remove(file);
					FileUtils.deleteQuietly(file);
					UsedFilesPart.sendFilesChanged(dataType);
				}
			}
		}
		this.selectRawFilesUI.updateViewer();
		WorkPart.updateSteps();
	}
}
