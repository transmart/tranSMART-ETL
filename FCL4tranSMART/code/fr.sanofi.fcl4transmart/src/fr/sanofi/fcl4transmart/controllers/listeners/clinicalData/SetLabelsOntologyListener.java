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
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.controllers.FileHandler;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetLabelsOntologyUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class SetLabelsOntologyListener implements Listener{
	private SetLabelsOntologyUI setLabelsOntologyUI;
	private DataTypeItf dataType;
	public SetLabelsOntologyListener(SetLabelsOntologyUI setLabelsOntologyUI, DataTypeItf dataType){
		this.dataType=dataType;
		this.setLabelsOntologyUI=setLabelsOntologyUI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		Vector<String> headers=this.setLabelsOntologyUI.getHeaders();
		Vector<String> newLabels=this.setLabelsOntologyUI.getNewLabels();
		Vector<String> codes=this.setLabelsOntologyUI.getCodes();
		
		if(((ClinicalData)this.dataType).getCMF()==null){
			this.setLabelsOntologyUI.displayMessage("Error: no column mapping file");
			return;
		}
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns.tmp");
		try{			  
			  FileWriter fw = new FileWriter(file);
			  BufferedWriter out = new BufferedWriter(fw);
			  out.write("Filename\tCategory Code\tColumn Number\tData Label\tData Label Source\tControlled Vocab Code\n");
				try{
					BufferedReader br=new BufferedReader(new FileReader(((ClinicalData)this.dataType).getCMF()));
					String line=br.readLine();
					while ((line=br.readLine())!=null){
						if(line.split("\t", -1)[3].compareTo("SUBJ_ID")==0 || line.split("\t", -1)[3].compareTo("VISIT_NAME")==0 || line.split("\t", -1)[3].compareTo("SITE_ID")==0 || line.split("\t", -1)[3].compareTo("\\")==0  || line.split("\t", -1)[3].compareTo("DATA_LABEL")==0 || line.split("\t", -1)[3].compareTo("OMIT")==0){
							out.write(line+"\n");
						}
						else{
							File rawFile=new File(this.dataType.getPath()+File.separator+line.split("\t", -1)[0]);
							String header=FileHandler.getColumnByNumber(rawFile, Integer.parseInt(line.split("\t", -1)[2]));
							String newLabel=newLabels.elementAt(headers.indexOf(rawFile.getName()+" - "+header));
							if(newLabel.compareTo("")==0){
								newLabel=header;
							}
							out.write(line.split("\t", -1)[0]+"\t"+line.split("\t", -1)[1]+"\t"+line.split("\t", -1)[2]+"\t"+newLabel+"\t\t"+codes.elementAt(headers.indexOf(rawFile.getName()+" - "+header))+"\n");

						}
					}
					br.close();
				}catch (Exception e){
					this.setLabelsOntologyUI.displayMessage("Error: "+e.getLocalizedMessage());
					e.printStackTrace();
					out.close();
				}
				
				out.close();
				String fileName=((ClinicalData)this.dataType).getCMF().getName();
				FileUtils.deleteQuietly(((ClinicalData)this.dataType).getCMF());
				try{
					File fileDest=new File(this.dataType.getPath()+File.separator+fileName);
					FileUtils.moveFile(file, fileDest);
					((ClinicalData)this.dataType).setCMF(fileDest);
				}
				catch(Exception ioe){
					this.setLabelsOntologyUI.displayMessage("File error: "+ioe.getLocalizedMessage());
					return;
				}
		  }catch (Exception e){
			  this.setLabelsOntologyUI.displayMessage("Error: "+e.getLocalizedMessage());
			  e.printStackTrace();
		  }
		this.setLabelsOntologyUI.displayMessage("Column mapping file updated");
		WorkPart.updateSteps();
		WorkPart.updateFiles();
	}
}
