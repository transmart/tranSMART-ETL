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
package fr.sanofi.fcl4transmart.handlers;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.Vector;
import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ProgressBar;
import fr.sanofi.fcl4transmart.controllers.RetrieveData;
import fr.sanofi.fcl4transmart.controllers.StudySelectionController;
/**
 *This class controls the window to see database and workspace free disk space, called from menu
 */	
public class SpaceHandler {
	private boolean isSearching;
	private File workspace;
	private String total;
	private String free;
	private double percent;
	private boolean testMeta;
	private Vector<String> names;
	private Vector<Double> totalDb;
	private Vector<Double> freeDb;
	private Vector<Double> percentDb;
	@Execute
	public void execute(Display display) {
		Shell loadingShell=new Shell();
		loadingShell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		loadingShell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(loadingShell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(loadingShell, SWT.NONE);
		searching.setText("Searching...");
		loadingShell.open();
		this.isSearching=true;
		this.workspace=StudySelectionController.getWorkspace();
		this.names=new Vector<String>();
		this.totalDb=new Vector<Double>();
		this.freeDb=new Vector<Double>();
		this.percentDb=new Vector<Double>();
		new Thread(){
			public void run() {
				if(workspace!=null){
					total=String.valueOf(workspace.getTotalSpace()/1024/1024);
					free=String.valueOf(workspace.getFreeSpace()/1024/1024);
					percent=100.0-((Double.longBitsToDouble(workspace.getFreeSpace())/Double.longBitsToDouble(workspace.getTotalSpace()))*100.0);
				}
				testMeta=RetrieveData.testMetadataConnection();
				if(testMeta){
					try{
						Class.forName("org.postgresql.Driver");
						Connection con = DriverManager.getConnection(RetrieveData.getConnectionString(), PreferencesHandler.getBiomartUser(), PreferencesHandler.getBiomartPwd());
						Statement stmt=con.createStatement();
						ResultSet rs=stmt.executeQuery("SELECT df.tablespace_name TABLESPACE, df.total_space TOTAL_SPACE, "+
										"fs.free_space FREE_SPACE, df.total_space_mb TOTAL_SPACE_MB, "+
										"(df.total_space_mb - fs.free_space_mb) USED_SPACE_MB, "+
										"fs.free_space_mb FREE_SPACE_MB, "+
										"ROUND(100 * (fs.free_space / df.total_space),2) PCT_FREE "+
										"FROM (SELECT tablespace_name, SUM(bytes) TOTAL_SPACE, "+
										"ROUND(SUM(bytes) / 1048576) TOTAL_SPACE_MB "+
										"FROM dba_data_files "+
										"GROUP BY tablespace_name) df, "+
										"(SELECT tablespace_name, SUM(bytes) FREE_SPACE, "+
										"ROUND(SUM(bytes) / 1048576) FREE_SPACE_MB "+
										"FROM dba_free_space "+
										"GROUP BY tablespace_name) fs "+
										"WHERE df.tablespace_name = fs.tablespace_name(+) "+
										"AND (df.tablespace_name='BIOMART' OR df.tablespace_name='DEAPP' OR df.tablespace_name='I2B2_DATA' OR df.tablespace_name='TRANSMART') "+
										"ORDER BY fs.tablespace_name");
						while(rs.next()){

							names.add(rs.getString("TABLESPACE"));
							totalDb.add(rs.getDouble("TOTAL_SPACE_MB"));
							freeDb.add(rs.getDouble("FREE_SPACE_MB"));
							percentDb.add((rs.getDouble("USED_SPACE_MB")/rs.getDouble("TOTAL_SPACE_MB"))*100);
						}
						
						con.close();
						
					}
					catch(SQLException e){
						e.printStackTrace();
					} catch (ClassNotFoundException e) {
						e.printStackTrace();
					}		
				}
				isSearching=false;
			}
        }.start();
        while(this.isSearching){
        	if (!display.readAndDispatch()) {
                display.sleep();
              }	
        }
        loadingShell.close();
        
		Shell shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    shell.setSize(500,400);
	    shell.setText("Free space");
	    gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		
		Group workspacePart=new Group(shell, SWT.SHADOW_ETCHED_IN);
		workspacePart.setText("Workspace disk space");
		workspacePart.setLayoutData(new GridData(GridData.FILL_BOTH));
		gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		gridLayout.horizontalSpacing=5;
		gridLayout.verticalSpacing=5;
		workspacePart.setLayout(gridLayout);
		
		if(workspace==null){
			Label warn=new Label(workspacePart, SWT.NONE);
			warn.setText("No workspace has been defined");
		}
		else{
			Label path=new Label(workspacePart, SWT.NONE);
			path.setText("Workspace path: "+this.workspace.getAbsolutePath());
			Composite grid=new Composite(workspacePart, SWT.NONE);
			gridLayout=new GridLayout();
			gridLayout.numColumns=5;
			gridLayout.horizontalSpacing=5;
			gridLayout.verticalSpacing=5;
			grid.setLayout(gridLayout);
			grid.setLayoutData(new GridData(GridData.FILL_BOTH));
			
			Label l1=new Label(grid, SWT.NONE);
			l1.setText("Name");
			Label l2=new Label(grid, SWT.NONE);
			l2.setText("Total space");
			Label l3=new Label(grid, SWT.NONE);
			l3.setText("Free space");
			Label l4=new Label(grid, SWT.NONE);
			l4.setText("Percent of used space");
			Label l5=new Label(grid, SWT.NONE);
			l5.setText("");
			
			Label name=new Label(grid, SWT.NONE);
			name.setText(workspace.getName());
			Label total=new Label(grid, SWT.NONE);
			total.setText(this.total + " Mb");
			Label free=new Label(grid, SWT.NONE);
			free.setText(this.free + " Mb");
			ProgressBar bar=new ProgressBar(grid, SWT.SMOOTH);
			bar.setMinimum(0);
			bar.setMaximum(100);
			
			bar.setSelection((int)this.percent);
			Label percentStr=new Label(grid, SWT.NONE);
			DecimalFormat format=new DecimalFormat("#.##");
			percentStr.setText(format.format(this.percent)+"%");
		}
		
		Group dbPart=new Group(shell, SWT.SHADOW_ETCHED_IN);
		dbPart.setText("Database space");
		dbPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		gridLayout.horizontalSpacing=5;
		gridLayout.verticalSpacing=5;
		dbPart.setLayout(gridLayout);
		if(!this.testMeta){
			Label warn=new Label(dbPart, SWT.NONE);
			warn.setText("Connection to database is not possible");
		}
		else{
			Label dbName=new Label(dbPart, SWT.NONE);
			dbName.setText("Database: "+PreferencesHandler.getDbName());
			Composite dbSpacePart=new Composite(dbPart, SWT.NONE);
			dbSpacePart.setLayoutData(new GridData(GridData.FILL_BOTH));
			gridLayout=new GridLayout();
			gridLayout.numColumns=5;
			gridLayout.horizontalSpacing=5;
			gridLayout.verticalSpacing=5;
			dbSpacePart.setLayout(gridLayout);
			
			Label l1=new Label(dbSpacePart, SWT.NONE);
			l1.setText("Tablespace");
			Label l2=new Label(dbSpacePart, SWT.NONE);
			l2.setText("Total space");
			Label l3=new Label(dbSpacePart, SWT.NONE);
			l3.setText("Free space");
			Label l4=new Label(dbSpacePart, SWT.NONE);
			l4.setText("Percent of free space");
			Label l5=new Label(dbSpacePart, SWT.NONE);
			l5.setText("");
			
			for(int i=0; i<this.names.size(); i++){
				Label name=new Label(dbSpacePart, SWT.NONE);
				name.setText(this.names.get(i));
				Label total=new Label(dbSpacePart, SWT.NONE);
				total.setText(String.valueOf(this.totalDb.get(i))+" Mb");
				Label free=new Label(dbSpacePart, SWT.NONE);
				free.setText(String.valueOf(this.freeDb.get(i))+" Mb");
				double percent=this.percentDb.get(i);
				ProgressBar bar=new ProgressBar(dbSpacePart, SWT.SMOOTH);
				bar.setMinimum(0);
				bar.setMaximum(100);					
				bar.setSelection((int)percent);
				Label percentStr=new Label(dbSpacePart, SWT.NONE);
				DecimalFormat format=new DecimalFormat("#.##");
				percentStr.setText(format.format(percent)+"%");
			}
			
		}
		
		shell.open();
	}
}
