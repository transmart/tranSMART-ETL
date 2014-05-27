/*******************************************************************************
 * Copyright (c) 2012 Sanofi-Aventis Recherche et D�veloppement.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *    Sanofi-Aventis Recherche et D�veloppement - initial API and implementation
 ******************************************************************************/
package fr.sanofi.fcl4transmart.controllers.listeners.description;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import fr.sanofi.fcl4transmart.handlers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.workUI.description.LoadDescriptionUI;
import fr.sanofi.fcl4transmart.model.interfaces.StudyItf;
/**
 *This class controls the study description loading
 */	
public class LoadDescriptionListener implements Listener{
	private LoadDescriptionUI loadDescriptionUI;
	private StudyItf study;
	public LoadDescriptionListener(LoadDescriptionUI loadDescriptionUI){
		this.loadDescriptionUI=loadDescriptionUI;
		this.study=this.loadDescriptionUI.getStudy();
	}
	@Override
	public void handleEvent(Event event) {
		String topNode=this.study.getTopNode();
		if(topNode==null || topNode.compareTo("")==0){
			this.loadDescriptionUI.displayMessage("The top node of the study has to be set");
			return;
		}
		try{
			Class.forName("org.postgresql.Driver");
			String connectionString="jdbc:postgresql://"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+"/"+PreferencesHandler.getDbName();
			Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getMetadataUser(), PreferencesHandler.getMetadataPwd());
			Statement stmt = con.createStatement();
			
			//remove tags for this study before adding new ones
			boolean res=stmt.execute("delete from i2b2_tags where path='"+topNode+"'");
			
			//add new tags
			Vector<String> fields=this.loadDescriptionUI.getFields();
			Vector<String> values=this.loadDescriptionUI.getValues();
			int cnt=0;
			for(int i=0; i<fields.size(); i++){
				if(fields.get(i).compareTo("")!=0 && values.get(i).compareTo("")!=0){
					ResultSet rs = stmt.executeQuery("select max(tag_id) from i2b2_tags");
			    	int max_tag_id;
			    	if(rs.next()){
			    		max_tag_id=rs.getInt(1)+1;
			    	}
			    	else{
			    		max_tag_id=0;
			    	}
			    	res=stmt.execute("insert into i2b2_tags values("+
				    	Integer.toString(max_tag_id)+
				    	",'"+topNode+"'"+
				    	",'"+values.get(i)+"'"+
				    	",'"+fields.get(i)+"'"+
				    	","+cnt+")"
			    	);
			    	cnt++;
				}
			}

	    	con.close();
	    	this.loadDescriptionUI.displayMessage("Description has been loaded");
		    	
		}catch(SQLException sqle){
			this.loadDescriptionUI.displayMessage("SQL error: "+sqle.getLocalizedMessage());
			sqle.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			// TODO Auto-generated catch block
			this.loadDescriptionUI.displayMessage("Java error: Class not found exception");
			cnfe.printStackTrace();
		}
	}
}
