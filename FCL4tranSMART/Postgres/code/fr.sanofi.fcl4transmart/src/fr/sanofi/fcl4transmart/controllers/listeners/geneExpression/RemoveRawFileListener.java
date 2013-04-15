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
import java.io.File;
import java.io.IOException;
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import fr.sanofi.fcl4transmart.model.classes.dataType.GeneExpressionData;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.SelectRawFileUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.ui.parts.UsedFilesPart;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;
/**
 *This class controls a clinical data file removing
 */	
public class RemoveRawFileListener implements Listener{
	private SelectRawFileUI selectRawFilesUI;
	private DataTypeItf dataType;
	public RemoveRawFileListener(DataTypeItf dataType, SelectRawFileUI selectRawFilesUI){
		this.dataType=dataType;
		this.selectRawFilesUI=selectRawFilesUI;
	}
	@Override
	public void handleEvent(Event event) {
		Vector<File> files=this.selectRawFilesUI.getSelectedRemovedFile();
		if(files.size()<1){
			this.selectRawFilesUI.displayMessage("No file selected");
			return;
		}
		File mapping=((GeneExpressionData)this.dataType).getStsmf();
		boolean confirm=this.selectRawFilesUI.confirm("The column mapping file and the word mapping file will be removed consequently.\nAre you sure to remove these files?");
		for(File file: files){
			if(file==null){
				return;
			}
			if(((GeneExpressionData)this.dataType).getRawFiles().size()==files.size()){
				if(mapping!=null){
					if(confirm){
						((GeneExpressionData)this.dataType).setSTSMF(null);
						try {
							FileUtils.forceDelete(mapping);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							this.selectRawFilesUI.displayMessage("File error: "+e.getLocalizedMessage());
							e.printStackTrace();
						}
						
						((GeneExpressionData)this.dataType).getRawFiles().remove(file);
						FileUtils.deleteQuietly(file);
						UsedFilesPart.sendFilesChanged(dataType);
					}
				}
				else{
					if(confirm){
						((GeneExpressionData)this.dataType).getRawFiles().remove(file);
						FileUtils.deleteQuietly(file);
						UsedFilesPart.sendFilesChanged(dataType);
					}
				}
			}
		}
		this.selectRawFilesUI.updateViewer();
		WorkPart.updateSteps();
		WorkPart.updateFiles();
	}
}
