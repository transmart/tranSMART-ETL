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
package fr.sanofi.fcl4transmart.controllers.listeners.geneExpression;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;

import fr.sanofi.fcl4transmart.controllers.PreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadAnnotationUI;

public class CheckAnnotationListener implements Listener{
	private LoadAnnotationUI loadAnnotationUI;
	private boolean isLoaded;
	private String platformId;
	public CheckAnnotationListener(LoadAnnotationUI loadAnnotationUI){
		this.loadAnnotationUI=loadAnnotationUI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		this.platformId=this.loadAnnotationUI.getPlatformId();
		this.loadAnnotationUI.openSearchingShell();
		new Thread(){
			public void run() {
				try{
					Class.forName("oracle.jdbc.driver.OracleDriver");
					String connectionString="jdbc:oracle:thin:@"+PreferencesHandler.getDbServer()+":"+PreferencesHandler.getDbPort()+":"+PreferencesHandler.getDbName();
					Connection con = DriverManager.getConnection(connectionString, PreferencesHandler.getDeappUser(), PreferencesHandler.getDeappPwd());
					Statement stmt = con.createStatement();
				    ResultSet rs = stmt.executeQuery("SELECT distinct platform from de_gpl_info, de_mrna_annotation where platform='"+platformId+"' and gpl_id='"+platformId+"'");
		
				    if(rs.next()){
				    	isLoaded=true;
				    }
				    else{
				    	isLoaded=false;
				    }			
				}catch(SQLException sqle){
					loadAnnotationUI.displayMessage("SQL error: "+sqle.getLocalizedMessage());
					sqle.printStackTrace();
					isLoaded=false;
				}
				catch(ClassNotFoundException cnfe){
					loadAnnotationUI.displayMessage("Java error: Class not found exception");
					cnfe.printStackTrace();
					isLoaded=false;
				}
				loadAnnotationUI.setIsSearching(false);
			}
		}.start();
		this.loadAnnotationUI.waitForSearchingThread();
		if(this.isLoaded){
	    	loadAnnotationUI.displayLoaded();
		}
		else{
	    	loadAnnotationUI.addLoadPart();	
		}
	}
	

}
