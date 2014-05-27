package fr.sanofi.fcl4transmart.handlers;

import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.util.Vector;
import javax.inject.Inject;
import org.eclipse.e4.core.di.annotations.CanExecute;
import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.core.di.annotations.Optional;
import org.eclipse.e4.ui.di.UIEventTopic;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class CopyHandler {
	private WorkItf workItf;
	@Execute
	public void execute() {
		Clipboard clipboard=Toolkit.getDefaultToolkit().getSystemClipboard();
		Vector<Vector<String>> data=this.workItf.copy();
		String s="";
		if(data!=null){
			int l=0;
			for(Vector<String> v: data){
				if(v.size()>l){
					l=v.size();
				}
			}
			if(data!=null){
				for(int i=0; i<l; i++){
					for(int j=0; j<data.size(); j++){
						Vector<String> line=data.get(j);
						if(line!=null){
							if(j<data.size()-1){
								if(i<line.size()){
									s+=line.get(i)+"\t";
								}
								else{
									s+="\t";
								}
							}
							else{
								if(j==data.size()-1){
									if(i<line.size()){
										s+=line.get(i)+"\n";
									}
									else{
										s+="\n";
									}
								}
								else{
									if(i<line.size()){
										s+=line.get(i);
									}
								}
							}
						}
					}
				}
			}
		}
		StringSelection sel=new StringSelection(s);
		clipboard.setContents(sel, sel);
	}
	@CanExecute
	public boolean canExecute() {
		if(this.workItf!=null){
			return this.workItf.canCopy();
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
