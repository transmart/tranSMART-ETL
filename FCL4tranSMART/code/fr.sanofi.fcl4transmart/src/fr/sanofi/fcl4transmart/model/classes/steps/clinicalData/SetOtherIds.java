package fr.sanofi.fcl4transmart.model.classes.steps.clinicalData;

import fr.sanofi.fcl4transmart.model.classes.dataType.ClinicalData;
import fr.sanofi.fcl4transmart.model.classes.workUI.clinicalData.SetOtherIdsUI;
import fr.sanofi.fcl4transmart.model.interfaces.DataTypeItf;
import fr.sanofi.fcl4transmart.model.interfaces.StepItf;
import fr.sanofi.fcl4transmart.model.interfaces.WorkItf;

public class SetOtherIds implements StepItf{
	private WorkItf workUI;
	private DataTypeItf dataType;
	public SetOtherIds(DataTypeItf dataType){
		this.workUI=new SetOtherIdsUI(dataType);
		this.dataType=dataType;
	}
	@Override
	public WorkItf getWorkUI() {
		return this.workUI;
	}
	public String toString(){
		return "Set other identifiers (optional)";
	}
	@Override
	public boolean isRealized() {
		// TODO Auto-generated method stub
		return false;
	}
	public String getDescription(){
		return "This optional step allows chosing the column corresponding to the columns corresponding to site and visit identifiers."+
				"If a column mapping file is already existing, the identifiers are retrieved in this file and given as parameters by default";
	}
	public boolean isAvailable(){
		try{
			if(((ClinicalData)this.dataType).getRawFiles().size()<1){
				return false;
			}
			if(((ClinicalData)this.dataType).getCMF()==null){
				return false;
			}
			return true;
		}
		catch(NullPointerException e){
			return false;
		}
	}
}
