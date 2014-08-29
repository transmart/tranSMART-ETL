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
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetUnitsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls the units setting step
 */	
public class SetUnitsListener implements Listener{
	private SetUnitsUI setUnitsUI;
	private DataTypeItf dataType;
	public SetUnitsListener(SetUnitsUI setUnitsUI, DataTypeItf dataType){
		this.setUnitsUI=setUnitsUI;
		this.dataType=dataType;
	}
	@Override
	public void handleEvent(Event event) {
		Vector<String> columns=this.setUnitsUI.getColumns();
		Vector<String> units=this.setUnitsUI.getUnits();
		if(columns.size()==1){
			if(columns.get(0).compareTo("")==0 && columns.get(0).compareTo("")==0){
				columns=new Vector<String>();
				units=new Vector<String>();
			}
		}
		for(int i=0; i<columns.size(); i++){
			if(columns.get(i).compareTo("")==0 || units.get(i).compareTo("")==0){
				this.setUnitsUI.displayMessage("Some values are not set");
				return;
			}
			String columnFileName=columns.get(i).split(" - ", 2)[0];
			String unitFileName=units.get(i).split(" - ", 2)[0];
			if(columnFileName.compareTo(unitFileName)!=0){
				this.setUnitsUI.displayMessage("Columns for value and unit have to been from the same file");
			}
		}
		if(((ClinicalData)this.dataType).getCMF()==null){
			this.setUnitsUI.displayMessage("Error: no column mapping file");
			return;
		}
		if(!this.checkValues(columns, units)) return;
		File file=new File(this.dataType.getPath().toString()+File.separator+this.dataType.getStudy().toString()+".columns.tmp");
		try{			  
			  FileWriter fw = new FileWriter(file);
			  BufferedWriter out = new BufferedWriter(fw);
			  out.write("Filename\tCategory Code\tColumn Number\tData Label\tData Label Source\tControlled Vocab Code\n");
				try{
					BufferedReader br=new BufferedReader(new FileReader(((ClinicalData)this.dataType).getCMF()));
					String line=br.readLine();
					while ((line=br.readLine())!=null){
						if(line.split("\t", -1)[3].compareTo("UNITS")!=0){
							out.write(line+"\n");
						}
					}
					br.close();
				}catch (Exception e){
					this.setUnitsUI.displayMessage("Error: "+e.getLocalizedMessage());
					e.printStackTrace();
					out.close();
				}
				for(int i=0; i<columns.size(); i++){
					String fileName=columns.get(i).split(" - ", 2)[0];
					int columnColumnNumber=-1;
					for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
						if(rawFile.getName().compareTo(fileName)==0){
							columnColumnNumber=FileHandler.getHeaderNumber(rawFile, columns.get(i).split(" - ", 2)[1]);
						}
					}
					int unitColumnNumber=-1;
					for(File rawFile: ((ClinicalData)this.dataType).getRawFiles()){
						if(rawFile.getName().compareTo(fileName)==0){
							unitColumnNumber=FileHandler.getHeaderNumber(rawFile, units.get(i).split(" - ", 2)[1]);
						}
					}
					if(columnColumnNumber!=-1 && unitColumnNumber!=-1){
						out.write(fileName+"\t\t"+String.valueOf(unitColumnNumber)+"\tUNITS\t"+String.valueOf(columnColumnNumber)+"\t\t\n");
								
					}
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
					this.setUnitsUI.displayMessage("File error: "+ioe.getLocalizedMessage());
					return;
				}
		  }catch (Exception e){
			  this.setUnitsUI.displayMessage("Error: "+e.getLocalizedMessage());
			  e.printStackTrace();
		  }
		this.setUnitsUI.displayMessage("Column mapping file updated");
		WorkPart.updateSteps();
		WorkPart.updateFiles();
	}
	private boolean checkValues(Vector<String> columns, Vector<String> units){
		File wmf=((ClinicalData)this.dataType).getWMF();
		for(int i=0; i<columns.size(); i++){
			File rawFile=new File(this.dataType.getPath()+File.separator+columns.get(i).split(" - ", 2)[0]);
			String headColumn=columns.get(i).split(" - ",2)[1];
			String headUnit=units.get(i).split(" - ", 2)[1];
			int numberColumn=FileHandler.getHeaderNumber(rawFile, headColumn);
			if(!FileHandler.isColumnNumerical(rawFile, wmf, numberColumn)){
				this.setUnitsUI.displayMessage("Values have to be numerical (term mapping considered).\nAt least '"+headColumn+"' is not numerical");
				return false;
			}
			int numberUnit=FileHandler.getHeaderNumber(rawFile, headUnit);
			if(FileHandler.isColumnNumerical(rawFile, wmf, numberUnit)){
				this.setUnitsUI.displayMessage("Units have to be non numerical.\nAt lease '"+headUnit+"' is numerical");
				return false;
			}
		}
		return true;
	}
}
