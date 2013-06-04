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
/**
 *This class controls a clinical raw data file selection
 */	
public class SelectClinicalRawFileListener implements Listener{
	private SelectRawFilesUI selectRawFilesUI;
	private DataTypeItf dataType;
	public SelectClinicalRawFileListener(SelectRawFilesUI selectRawFilesUI, DataTypeItf dataType){
		this.selectRawFilesUI=selectRawFilesUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		this.selectRawFilesUI.openLoadingShell();
		new Thread(){
			public void run() {
				String[] paths=selectRawFilesUI.getPath().split(File.pathSeparator, -1);
				for(int i=0; i<paths.length; i++){
					String path=paths[i];
					if(path==null){
						selectRawFilesUI.setIsLoading(false);
						return;
					}
					File rawFile=new File(path);
					if(rawFile.exists()){
						if(rawFile.isFile()){
							if(path.contains("%")){
								selectRawFilesUI.setMessage("File name can not contain percent ('%') symbol.");
								selectRawFilesUI.setIsLoading(false);
								return;
							}
							String newPath=dataType.getPath().getAbsolutePath()+File.separator+rawFile.getName();
							if(selectRawFilesUI.getFormat().compareTo("Tab delimited raw file")!=0 && selectRawFilesUI.getFormat().compareTo("SOFT")!=0){
								selectRawFilesUI.setMessage("File format does not exist");
								selectRawFilesUI.setIsLoading(false);
								return;
							}
							if(selectRawFilesUI.getFormat().compareTo("SOFT")==0){
								File newFile=new File(newPath);
								if(newFile.exists()){
									selectRawFilesUI.setMessage("File has already been added");
									selectRawFilesUI.setIsLoading(false);
									return;
								}else{
									if(createTabFileFromSoft(rawFile, newFile)){
										((ClinicalData)dataType).addRawFile(newFile);
										selectRawFilesUI.setMessage("File has been added");
									}
								}
							}
							else if(selectRawFilesUI.getFormat().compareTo("Tab delimited raw file")==0){
								if(!checkTabFormat(rawFile)){
									selectRawFilesUI.setIsLoading(false);
									return;
								}
	
								File copiedRawFile=new File(newPath);
								if(!copiedRawFile.exists()){
									try {
										FileUtils.copyFile(rawFile, copiedRawFile);
										((ClinicalData)dataType).addRawFile(copiedRawFile);
										selectRawFilesUI.setMessage("File has been added");
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
										selectRawFilesUI.setMessage("File error: "+e.getLocalizedMessage());
										selectRawFilesUI.setIsLoading(false);
										return;
									}
								}
								else{
									selectRawFilesUI.setMessage("File has already been added");
									selectRawFilesUI.setIsLoading(false);
									return;
								}
							}
	
						}
						else{
							selectRawFilesUI.setMessage("File is a directory");
							selectRawFilesUI.setIsLoading(false);
							return;
						}
					}
					else{
						selectRawFilesUI.setMessage("Path does no exist");
						selectRawFilesUI.setIsLoading(false);
						return;
					}
				}
				selectRawFilesUI.setIsLoading(false);
			}
		}.start();
		this.selectRawFilesUI.waitForThread();
		selectRawFilesUI.updateViewer();
		WorkPart.updateSteps();
		UsedFilesPart.sendFilesChanged(dataType);
	}
	/**
	 *Checks the format of a tab delimited raw data file
	 */	
	public boolean checkTabFormat(File rawFile){
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			int columnsNbr=line.split("\t", -1).length;
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					if(line.split("\t", -1).length!=columnsNbr){
						selectRawFilesUI.setMessage("Wrong file format:\nLines have no the same number of columns");
						selectRawFilesUI.setIsLoading(false);
						br.close();
						return false;
					}
				}
			}
			br.close();
		}catch (Exception e){
			selectRawFilesUI.setMessage("File error: "+e.getLocalizedMessage());
			e.printStackTrace();
			return false;
		}
		return true;
	}
	/**
	 *Creates a tab delimited file from a SOFT file
	 */	
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
						lines.get(lines.size()-1).put("sample", line.split(".SAMPLE = ", -1)[1]);
					}
					else if(m2.matches()){
						String s=line.split("!Sample_characteristics_ch. = ", -1)[1];
						String tag=s.split(": ", -1)[0];
						if(!columns.contains(tag)){
							columns.add(tag);
						}
						lines.get(lines.size()-1).put(tag, s.split(": ", -1)[1]);
					}
				}
			}
			br.close();
		}catch (Exception e){
			selectRawFilesUI.setMessage("File error: "+e.getLocalizedMessage());
			e.printStackTrace();
		}
		if(columns.size()<=1){
			selectRawFilesUI.setMessage("Wrong soft format: no characteristics");
			selectRawFilesUI.setIsLoading(false);
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
			selectRawFilesUI.setMessage("File error: "+e.getLocalizedMessage());
			selectRawFilesUI.setIsLoading(false);
			e.printStackTrace();
			return false;
		}
	}
}
