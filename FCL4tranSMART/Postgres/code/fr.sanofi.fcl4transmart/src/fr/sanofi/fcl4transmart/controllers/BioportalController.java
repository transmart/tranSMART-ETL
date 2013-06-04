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
package fr.sanofi.fcl4transmart.controllers;

import java.io.InputStream;
import java.net.Authenticator;
import java.net.ProxySelector;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.eclipse.jface.viewers.ArrayContentProvider;
import org.eclipse.jface.viewers.ColumnLabelProvider;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.jface.viewers.TableViewerColumn;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.btr.proxy.search.ProxySearch;

import fr.sanofi.fcl4transmart.handlers.ProxyPreferencesHandler;
import fr.sanofi.fcl4transmart.model.classes.Auth;
import fr.sanofi.fcl4transmart.ui.parts.WorkPart;

/**
 * This class allows opening a shell for Bioportal search, and get a set with term and code of an ontology item
 */
public class BioportalController {
	private static Shell shell;
	private static Text text;
	private static Label labelCount;
	private static TableViewer viewer;
	private static Table table;
	private static Combo combo;
	private static ArrayList<OntologyTerm> terms;
	private static String[] toReturn;
	private static boolean isSearching;
	private static boolean test;
	/**
	 * Test if connexion to Bioportal webservice is available
	 */
	public static boolean testBioportalConnection(){
		Shell shell=new Shell();
		shell.setSize(50, 100);
		GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		ProgressBar pb = new ProgressBar(shell, SWT.HORIZONTAL | SWT.INDETERMINATE);
		pb.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));

		Label searching=new Label(shell, SWT.NONE);
		searching.setText("Searching...");
		shell.open();
		isSearching=true;
		new Thread(){
			public void run() {
				test=true;
				try{
					if(ProxyPreferencesHandler.getMethod().compareTo("Native")==0){
						ProxySearch proxySearch = ProxySearch.getDefaultProxySearch();
						ProxySelector myProxySelector = proxySearch.getProxySelector();
						ProxySelector.setDefault(myProxySelector);
						
						URL url=new URL("http://rest.bioontology.org"); 
				        InputStream ins = url.openConnection().getInputStream();
						ins.close();
					}
					else if(ProxyPreferencesHandler.getMethod().compareTo("Manual")==0){
						if(ProxyPreferencesHandler.getHost()!=null && ProxyPreferencesHandler.getPort()!=null){
							if(ProxyPreferencesHandler.getAuthRequired()){
								Authenticator.setDefault(new Auth(ProxyPreferencesHandler.getUser(), ProxyPreferencesHandler.getPass()));
							}
							System.setProperty("http.proxyHost", ProxyPreferencesHandler.getHost());  
							System.setProperty("http.proxyPort", ProxyPreferencesHandler.getPort()); 
							URL url=new URL("http://rest.bioontology.org"); 
					        InputStream ins = url.openConnection().getInputStream();
							ins.close();
							System.setProperty("http.proxyHost", "");  
							System.setProperty("http.proxyPort", "");  
						}
					}else{
						URL url=new URL("http://rest.bioontology.org"); 
				        InputStream ins = url.openConnection().getInputStream();
						ins.close();
					}
				}
				catch(Exception e){
					e.printStackTrace();
					test=false;
				}

				isSearching=false;
			}
	    }.start();
	    Display display=WorkPart.display();
	    while(isSearching){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }	
	    }
	    shell.close();
	    return test;
	}
	/**
	 * Create the search shell, and return the term and code of a selected ontology item under the form of an array with two potentially null strings
	 */
	public static String[] getTerms(){
		toReturn=new String[2];
		shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    shell.setSize(800,600);
	    shell.setText("Bioportal search");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		shell.setLayout(gridLayout);
		
		Composite head=new Composite(shell, SWT.NONE);
		gridLayout=new GridLayout();
		gridLayout.numColumns=4;
		gridLayout.horizontalSpacing=5;
		gridLayout.verticalSpacing=5;
		head.setLayout(gridLayout);
		GridData gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		head.setLayoutData(gridData);
		
		Label label=new Label(head, SWT.NONE);
		label.setText("Search: ");
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		label.setLayoutData(gridData);
		
		text=new Text(head, SWT.BORDER);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.widthHint=100;
		text.setLayoutData(gridData);
		
		combo=new Combo(head, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
	    combo.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
	    
	    final HashMap<String, String> ontologies=getOntologies();
	    combo.add("All ontologies");
	    combo.add("----------");
	    if(ontologies.keySet().contains("MedDRA")){
	    	combo.add("MedDRA");
	    }
	    if(ontologies.keySet().contains("SNOMED Clinical Terms")){
	    	combo.add("SNOMED Clinical Terms");
	    }
	    if(ontologies.keySet().contains("Medical Subject Headings (MeSH)")){
	    	combo.add("Medical Subject Headings (MeSH)");
	    }
	    combo.add("----------");
	    for(String key: ontologies.keySet()){
	    	combo.add(key+"("+ontologies.get(key)+")");
	    }
	    combo.select(0);

	    Button button=new Button(head, SWT.PUSH);
	    button.setText("Search");
	    button.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				searchTerms(ontologies);
			}
	    });
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		button.setLayoutData(gridData);
	    
		terms=new ArrayList<OntologyTerm>(); 
		terms.add(new OntologyTerm("","","","","",""));
	    labelCount=new Label(shell, SWT.NONE);
	    labelCount.setText("Number of results: 0");
	    gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		labelCount.setLayoutData(gridData);
	    
	    Composite container=new Composite(shell, SWT.NONE);
	    gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		gridLayout.horizontalSpacing=5;
		gridLayout.verticalSpacing=5;
		container.setLayout(gridLayout);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		gridData.heightHint=300;
		container.setLayoutData(gridData);

	    viewer = new TableViewer(container, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL | SWT.FULL_SELECTION | SWT.BORDER);

		createColumns();
		table=viewer.getTable();
		table.setHeaderVisible(true);
		table.setLinesVisible(true); 
		viewer.setContentProvider(new ArrayContentProvider());

		viewer.setInput(terms);
		viewer.getControl().setLayoutData(new GridData(GridData.FILL_BOTH));
		shell.layout(true, true);
	    
		Composite buttonsPart=new Composite(shell, SWT.NONE);
		gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		gridLayout.horizontalSpacing=5;
		gridLayout.verticalSpacing=5;
		buttonsPart.setLayout(gridLayout);
		gridData = new GridData();
		gridData.horizontalAlignment = SWT.FILL;
		gridData.grabExcessHorizontalSpace = true;
		buttonsPart.setLayoutData(gridData);
		
		Button ok=new Button(buttonsPart, SWT.PUSH);
		ok.setText("Select");
		ok.addListener(SWT.Selection, new Listener(){
			public void handleEvent(Event event){
				int n=viewer.getTable().getSelectionIndex();
				if(n==-1){
					return;
				}
				toReturn[0]=terms.get(n).getTerm();
				toReturn[1]=terms.get(n).getCode();
				shell.close();
			}
		});
		
		Button cancel=new Button(buttonsPart, SWT.PUSH);
		cancel.setText("Cancel");
		cancel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				shell.close();
			}
		});
		
		shell.open();
		Display display = WorkPart.display();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}

		return toReturn;
	}
	/**
	 * Search all available ontologies in Bioportal and returns a HashMap with ontology name as key and ontology identifier as value
	 */	
	public static HashMap<String, String> getOntologies(){
		HashMap<String, String> ontologies=new HashMap<String, String>();
		String getLatestOntologiesUrl = "http://rest.bioontology.org/bioportal/ontologies?apikey=";
		String API_KEY = "cfaa37e7-5bd9-4e0f-954f-d9263ab46f5d";
		DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder;
		try {
			docBuilder = docBuilderFactory.newDocumentBuilder();
			Document doc = docBuilder.parse(getLatestOntologiesUrl+API_KEY);
			doc.getDocumentElement().normalize(); 

			doc.getElementsByTagName("success");
			NodeList listOfOntologies = doc.getElementsByTagName("ontologyBean");
			int totalNumberOfOntologies = listOfOntologies.getLength(); 

			if (totalNumberOfOntologies == 0) {
				return null;
			}
			else {
				for(int s=0; s<listOfOntologies.getLength(); s++) {
					Node ontologyBeanNode = listOfOntologies.item(s);
					if(ontologyBeanNode.getNodeType() == Node.ELEMENT_NODE){
						Element ontologyBeanElements = (Element)ontologyBeanNode;                    
						
						NodeList Node = ontologyBeanElements.getElementsByTagName("displayLabel");
						Element ontologyElement = (Element)Node.item(0);
						
						NodeList Node2 = ontologyBeanElements.getElementsByTagName("ontologyId");
						Element ontologyElement2 = (Element)Node2.item(0);
						
						ontologies.put(ontologyElement.getTextContent(), ontologyElement2.getTextContent());
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return ontologies;
	}
	/**
	 * Create the columns of the table viewer
	 */
	private static void createColumns() {
		String[] titles = { "Term", "Code", "Ontology", "Ontology version", "Synonyme", "Obsolete" };
		int[] bounds = { 100, 100, 100, 100, 100, 100 };

		TableViewerColumn col = createTableViewerColumn(titles[0], bounds[0], 0);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getTerm();
			}
		});

		col = createTableViewerColumn(titles[1], bounds[1], 1);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getCode();
			}
		});

		col = createTableViewerColumn(titles[2], bounds[2], 2);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getOntology();
			}
		});

		col = createTableViewerColumn(titles[3], bounds[3], 3);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getOntologyVersion();
			}
		});
		col = createTableViewerColumn(titles[4], bounds[4], 4);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getSynonyme();
			}
		});
		
		col = createTableViewerColumn(titles[5], bounds[5], 5);
		col.setLabelProvider(new ColumnLabelProvider() {
			@Override
			public String getText(Object element) {
				OntologyTerm o = (OntologyTerm) element;
				return o.getObsolete();
			}
		});

	}
	/**
	 * Create a column for the table viewer - called by the method createColumns()
	 */
	private static TableViewerColumn createTableViewerColumn(String title, int bound, final int colNumber) {
		final TableViewerColumn viewerColumn = new TableViewerColumn(viewer,
				SWT.NONE);
		final TableColumn column = viewerColumn.getColumn();
		column.setText(title);
		column.setWidth(bound);
		column.setResizable(true);
		column.setMoveable(true);
		return viewerColumn;
	}
	/**
	 * Search all terms for a given search, and fill a list with corresponding objects OntologyTerm
	 */
	private static void searchTerms(HashMap<String, String> ontologies){
		terms=new ArrayList<OntologyTerm>();
		if(combo.getText().compareTo("")==0 || text.getText().compareTo("----------")==0){
			return;
		}
		else{
			String BIOPORTAL_URL_PREFIX = "http://rest.bioontology.org/bioportal/search/";
			String API_KEY = "cfaa37e7-5bd9-4e0f-954f-d9263ab46f5d";
			String searchParameters;
			if(combo.getText().compareTo("All ontologies")==0){
				searchParameters = "?isexactmatch=0"+"&includeproperties=0&apikey="+API_KEY;
			}
			else{
				String ONTOLOGY_ID=ontologies.get(combo.getText());
				searchParameters = "?ontologyids="+ONTOLOGY_ID+"&isexactmatch=0"+"&includeproperties=0&apikey="+API_KEY;
			}
			String uri = BIOPORTAL_URL_PREFIX+text.getText()+searchParameters;
			try{
				DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
	
				Document doc = docBuilder.parse(uri);
				doc.getDocumentElement().normalize(); 
	
				doc.getElementsByTagName("success");
				NodeList listOfSearchResults = doc.getElementsByTagName("searchBean");
				for(int s=0; s<listOfSearchResults.getLength(); s++) {
					String term="", code="", ontology="", ontologyVersion="", synonyme="", obsolete="";
					Node searchBeanNode = listOfSearchResults.item(s);
					if(searchBeanNode.getNodeType() == Node.ELEMENT_NODE){
						Element ontologyBeanElements = (Element)searchBeanNode;
						NodeList node = ontologyBeanElements.getElementsByTagName("preferredName");
						Element ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							term = ontologyElement.getTextContent();
						}
						node = ontologyBeanElements.getElementsByTagName("conceptIdShort");
						ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							code = ontologyElement.getTextContent();
						}
						node = ontologyBeanElements.getElementsByTagName("ontologyDisplayLabel");
						ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							ontology = ontologyElement.getTextContent();
						}
						node = ontologyBeanElements.getElementsByTagName("ontologyVersionId");
						ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							ontologyVersion = ontologyElement.getTextContent();
						}
						node = ontologyBeanElements.getElementsByTagName("recordType");
						ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							if(ontologyElement.getTextContent().compareTo("apreferredname")==0){
								synonyme="Preferred";
							}
							else{
								synonyme="Synonyme";
							}
						}
						node = ontologyBeanElements.getElementsByTagName("isObsolete");
						ontologyElement = (Element)node.item(0);	
						if (ontologyElement != null) {
							if(ontologyElement.getTextContent().compareTo("0")==0){
								obsolete="False";
							}
							else{
								obsolete="True";
							}
						}
					}
					terms.add(new OntologyTerm(term, code, ontology, ontologyVersion, synonyme, obsolete));
				}		
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		if(terms.size()==0){
			terms.add(new OntologyTerm("","","","","",""));
		    labelCount.setText("Number of results: 0");
		}
		else{
			labelCount.setText("Number of results: "+String.valueOf(terms.size()));
		}
		viewer.setInput(terms);
		viewer.refresh();
		viewer.getControl().pack();
		shell.layout(true, true);
	}
}
