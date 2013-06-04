package fr.sanofi.fcl4transmart.handlers;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.DataFlavor;
import java.util.Vector;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.CanExecute;
import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.ui.di.UIEventTopic;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class MapFromClipboardHandler {
	private WorkItf workItf;
	@Execute
	public void execute() {
		Vector<Vector<String>> data=new Vector<Vector<String>>();
		Clipboard clipboard=Toolkit.getDefaultToolkit().getSystemClipboard();
		String  s;
		try {
			s=(String)(clipboard.getContents(this).getTransferData(DataFlavor.stringFlavor));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		 String[] lines = s.split("\n", -1);
		 int n=0;
		 for(int i=0; i<lines.length; i++){
			 if(n<(lines[i].split("\t", -1).length)){
				 n=lines[i].split("\t", -1).length;
			 }
		 }
		 for(int i=0; i<n; i++){
			 data.add(new Vector<String>());
		 }
         for (int i=0 ; i<lines.length; i++) {
        	 if(i==lines.length-1 && lines[i].compareTo("")==0) break;
             String[] cells = lines[i].split("\t", -1);
             for (int j=0 ; j<n; j++) { 
                 if(j<cells.length){
                	 data.get(j).add(cells[j]);
                 }
                 else{
                	 data.get(j).add("");
                 }
             } 
         } 
         this.workItf.mapFromClipboard(data);
	}
	@CanExecute
	public boolean canExecute() {
		if(this.workItf!=null){
			return this.workItf.canPaste();
		}
		return false;
	}
	@Inject
	void eventReceived(@Optional @UIEventTopic("stepChanged/*") StepItf selectedStep) {
		if(selectedStep!=null){
			this.workItf=selectedStep.getWorkUI();
		}
	} 
}
