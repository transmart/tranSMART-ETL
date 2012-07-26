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
import java.util.HashMap;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SelectRawFilesUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

public class SelectClinicalRawFileListener implements Listener{
	private SelectRawFilesUI selectRawFilesUI;
	private DataTypeItf dataType;
	public SelectClinicalRawFileListener(SelectRawFilesUI selectRawFilesUI, DataTypeItf dataType){
		this.selectRawFilesUI=selectRawFilesUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		String path=this.selectRawFilesUI.getPath();
		if(path==null) return;
		File rawFile=new File(path);
		if(rawFile.exists()){
			if(rawFile.isFile()){
				String newPath=this.dataType.getPath().getAbsolutePath()+File.separator+rawFile.getName();
				if(this.selectRawFilesUI.getFormat().compareTo("Tab delimited raw file")!=0 && this.selectRawFilesUI.getFormat().compareTo("SOFT")!=0){
					this.selectRawFilesUI.displayMessage("This file format does not exist");
					return;
				}
				if(this.selectRawFilesUI.getFormat().compareTo("SOFT")==0){
					File newFile=new File(newPath);
					if(newFile.exists()){
						this.selectRawFilesUI.displayMessage("This file has already been added");
					}else{
						if(this.createTabFileFromSoft(rawFile, newFile)){
							((ClinicalData)this.dataType).addRawFile(newFile);
							this.selectRawFilesUI.displayMessage("File has been added");
							this.selectRawFilesUI.updateViewer();
							WorkPart.updateSteps();
							//to do: update files list
							UsedFilesPart.sendFilesChanged(dataType);
						}
					}
				}
				else if(this.selectRawFilesUI.getFormat().compareTo("Tab delimited raw file")==0){
					if(!this.checkTabFormat(rawFile)) return;

					File copiedRawFile=new File(newPath);
					if(!copiedRawFile.exists()){
						try {
							FileUtils.copyFile(rawFile, copiedRawFile);
							((ClinicalData)this.dataType).addRawFile(copiedRawFile);
							this.selectRawFilesUI.displayMessage("File has been added");
							this.selectRawFilesUI.updateViewer();
							WorkPart.updateSteps();
							//to do: update files list
							UsedFilesPart.sendFilesChanged(dataType);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					else{
						this.selectRawFilesUI.displayMessage("This file has already been added");
					}
				}

			}
			else{
				this.selectRawFilesUI.displayMessage("This is a directory");
			}
		}
		else{
			this.selectRawFilesUI.displayMessage("This path does no exist");
		}
	}
	public boolean checkTabFormat(File rawFile){
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			int columnsNbr=line.split("\t", 20).length;
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					if(line.split("\t",20).length!=columnsNbr){
						this.selectRawFilesUI.displayMessage("Wrong file format:\nLines have no the same number of columns");
						br.close();
						return false;
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
	public boolean createTabFileFromSoft(File rawFile, File newFile){
		Vector<String> columns=new Vector<String>();
		Vector<HashMap<String, String>> lines=new Vector<HashMap<String, String>>();	
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line;
			Pattern p1=Pattern.compile(".SAMPLE = .*");
			Pattern p2=Pattern.compile("!Sample_characteristics_ch. = .*: .*");
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					Matcher m1=p1.matcher(line);
					Matcher m2=p2.matcher(line);
					if(m1.matches()){
						lines.add(new HashMap<String, String>());
						if(!columns.contains("sample")){
							columns.add("sample");
						}
						lines.get(lines.size()-1).put("sample", line.split(".SAMPLE = ",2)[1]);
					}
					else if(m2.matches()){
						String s=line.split("!Sample_characteristics_ch. = ",2)[1];
						String tag=s.split(": ",2)[0];
						if(!columns.contains(tag)){
							columns.add(tag);
						}
						lines.get(lines.size()-1).put(tag, s.split(": ", 2)[1]);
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if(columns.size()<=1){
			this.selectRawFilesUI.displayMessage("Wrong soft format: no characteristics");
			return false;
		}
		FileWriter fw;
		try {
			fw = new FileWriter(newFile);
			BufferedWriter out = new BufferedWriter(fw);
	
			for(int i=0; i<columns.size()-1; i++){
				out.write(columns.get(i)+"\t");
			}
			out.write(columns.get(columns.size()-1)+"\n");
			
			for(HashMap<String, String> sample: lines){
				for(int i=0; i<columns.size()-1; i++){
					String value=sample.get(columns.get(i));
					if(value==null) value="";
					out.write(value+"\t");
				}
				String value=sample.get(columns.get(columns.size()-1));
				if(value==null) value="";
				out.write(value+"\n");
			}
			out.close();
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
}
