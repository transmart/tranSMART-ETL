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

import fr.sanofi.fcl4transmart.model.classes.workUI.geneExpression.LoadAnnotationUI;

public class CheckAnnotationListener implements Listener{
	private LoadAnnotationUI loadAnnotationUI;
	public CheckAnnotationListener(LoadAnnotationUI loadAnnotationUI){
		this.loadAnnotationUI=loadAnnotationUI;
	}
	@Override
	public void handleEvent(Event event) {
		// TODO Auto-generated method stub
		String platformId=this.loadAnnotationUI.getPlatformId();
		try{
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection con = DriverManager.getConnection("jdbc:oracle:thin:@TLDS06VZ:1510:TLDET10", "deapp", "deapp");
			Statement stmt = con.createStatement();
		    ResultSet rs = stmt.executeQuery("SELECT distinct platform from de_gpl_info, de_mrna_annotation where platform='"+platformId+"' and gpl_id='"+platformId+"'");

		    if(rs.next()){
		    	this.loadAnnotationUI.displayLoaded();
		    }
		    else{
		    	this.loadAnnotationUI.addLoadPart();
		    	
		    }

			
		}catch(SQLException sqle){
			sqle.printStackTrace();
		}
		catch(ClassNotFoundException cnfe){
			cnfe.printStackTrace();
		}


	}
	

}
