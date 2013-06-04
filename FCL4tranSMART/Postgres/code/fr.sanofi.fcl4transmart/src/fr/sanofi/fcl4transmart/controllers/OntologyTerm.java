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
/**
 *This class represents an ontology term
 */
public class OntologyTerm {
	private String term;
	private String code;
	private String ontology;
	private String ontologyVersion;
	private String synonyme;
	private String obsolete;
	public OntologyTerm(String term, String code, String ontology, String ontologyVersion, String synonyme, String obsolete){
		this.term=term;
		this.code=code;
		this.ontology=ontology;
		this.ontologyVersion=ontologyVersion;
		this.synonyme=synonyme;
		this.obsolete=obsolete;
	}
	public String getTerm(){
		return this.term;
	}
	public String getCode(){
		return this.code;
	}
	public String getOntology(){
		return this.ontology;
	}
	public String getOntologyVersion(){
		return this.ontologyVersion;
	}
	public String getSynonyme(){
		return this.synonyme;
	}
	public String getObsolete(){
		return this.obsolete;
	}
}
