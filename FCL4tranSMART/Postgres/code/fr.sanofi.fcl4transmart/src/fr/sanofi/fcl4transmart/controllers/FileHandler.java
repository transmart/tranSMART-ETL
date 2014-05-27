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
package fr.sanofi.fcl4transmart.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Vector;
/**
 * This class offers methods to parse files (raw data and mapping files)
 */
public class FileHandler {
	private static String[] reserved_word={"SUBJ_ID", "OMIT", "VISIT_NAME", "VISIT_NAME_2", "SITE_ID"};
	private static boolean isReserved(String s){
		for(int i=0; i<FileHandler.reserved_word.length; i++){
			if(FileHandler.reserved_word[i].compareTo(s)==0){
				return true;
			}
		}
		return false;
	}
	/**
	 * Returns a vector with all headers of a clinical raw data file
	 */
	public static Vector<String> getHeaders(File file){
		Vector<String> headers=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			String[] s=line.split("\t", -1);
			for(int i=0; i<s.length; i++){
				headers.add(s[i]);
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return headers;
	}
	/**
	 * Returns the column number of a given header for a clinical data file
	 */
	public static int getHeaderNumber(File file, String string){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			String[] s=line.split("\t", -1);
			for(int i=0; i<s.length; i++){
				if(s[i].compareTo(string)==0){
					br.close();
					return i+1;
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return -1;	
	}
	/**
	 * Returns the count of columns for a raw clinical data file
	 */
	public static int getColumnsNumber(File file){
		int n=-1;
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			String[] s=line.split("\t", -1);
			n=s.length;
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return n;
	}

	/**
	 *Reads a column mapping file, and returns the column number for the line with a given label, for a given raw clinical data file 
	 */
	public static int getNumberForLabel(File file, String string, File rawFile){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] s=line.split("\t", -1);
					if(s[3].compareTo(string)==0 && s[0].compareTo(rawFile.getName())==0){//data label: third column
						try{
							br.close();
							return Integer.parseInt(s[2]);
						}catch(NumberFormatException nfe){
							br.close();
							return -1;
						}
					}
				}
			}
			
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return -1;
	}
	
	/**
	 *Reads a column mapping file, and returns the raw data file name for athe line with a given label
	 */
	public static String getRawForLabel(File file, String string){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[3].compareTo(string)==0){//data label: third column
					br.close();
					return s[0];
				}
			}
			
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return "";
	}

	/**
	 *Returns the header for the n-th column of data file 
	 */
	public static String getColumnByNumber(File file, int n){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			String[] s=line.split("\t", -1);
			br.close();
			return s[n-1];			
		}catch (Exception e){
			e.printStackTrace();
		}
		return "";
	}
	/**
	 *Returns a vector of string containing all headers of clinical raw data file that will be integrated in databases (non omited and not identifiers in cmf) 
	 */	
	public static Vector<String> getHeadersFromCmf(File cmf, File rawFile){
		String[] rawHeaders=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			rawHeaders=line.split("\t", -1);
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}
		Vector<String>headers=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(rawFile.getName())==0){
					if(!FileHandler.isReserved(s[3]) && s[3].compareTo("\\")!=0 && s[3].compareTo("MIN")!=0 && s[3].compareTo("MAX")!=0 && s[3].compareTo("MEAN")!=0 && s[3].compareTo("UNITS")!=0){
						if(!headers.contains(rawHeaders[Integer.parseInt(s[2])-1])){
							headers.add(rawHeaders[Integer.parseInt(s[2])-1]);
						}
					}
				}
			}
			
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return headers;
	}
	
	/**
	 *Returns a vector of string containing all headers of raw data that are not omitted
	 */	
		public static Vector<String> getNonOmittedHeaders(File cmf, File rawFile){
			String[] rawHeaders=null;
			try{
				BufferedReader br = new BufferedReader(new FileReader(rawFile));
				String line=br.readLine();
				rawHeaders=line.split("\t", -1);
				br.close();
			}catch (Exception e){
				e.printStackTrace();
				return null;
			}
			
			Vector<String>headers=new Vector<String>();
			try{
				BufferedReader br = new BufferedReader(new FileReader(cmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(s[0].compareTo(rawFile.getName())==0){
						if(s[3].compareTo("OMIT")!=0){
							if(!headers.contains(rawHeaders[Integer.parseInt(s[2])-1])){
								headers.add(rawHeaders[Integer.parseInt(s[2])-1]);
							}
						}
					}
				}
				
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
			return headers;
		}
	
	/**
	 *Returns the data label from a header of raw clinical data
	 */	
	public static String getDataLabel(File cmf, File rawFile, String header){
		String[] rawHeaders=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			rawHeaders=line.split("\t", -1);
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
		int columnNumber=-1;
		for(int i=0; i<rawHeaders.length; i++){
			if(rawHeaders[i].compareTo(header)==0) columnNumber=i;
		}
		if(columnNumber!=-1){
			try{
				BufferedReader br = new BufferedReader(new FileReader(cmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(s[0].compareTo(rawFile.getName())==0 && (Integer.parseInt(s[2])-1)==columnNumber){
						if(s[3].compareTo("MIN")!=0 && s[3].compareTo("MAX")!=0 && s[3].compareTo("MEAN")!=0 && s[3].compareTo("UNITS")!=0){
							br.close();
							return s[3];
						}
					}
				}
				
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		return "";
	}
	
	/**
	 *Returns the controlled vocabulary code for a given header of a raw clinical data file
	 */	
	public static String getCodeFromHeader(File cmf, File rawFile, String header){
		String[] rawHeaders=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			rawHeaders=line.split("\t", -1);
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
		int columnNumber=-1;
		for(int i=0; i<rawHeaders.length; i++){
			if(rawHeaders[i].compareTo(header)==0) columnNumber=i;
		}
		if(columnNumber!=-1){
			try{
				BufferedReader br = new BufferedReader(new FileReader(cmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if((Integer.parseInt(s[2])-1)==columnNumber){
						br.close();
						return s[5];
					}
				}
				
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		return "";
	}
	
	/**
	 *Checks if a new data value exists for a given old data value, for one header of a clinical raw data file
	 */	
	public static String getNewDataValue(File wmf, File rawFile, String header, String oldData){
		if(wmf==null) return null;
		try{
			int columnNumber=FileHandler.getHeaderNumber(rawFile, header);
			BufferedReader br = new BufferedReader(new FileReader(wmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(rawFile.getName())==0 && Integer.parseInt(s[1])==columnNumber && s[2].compareTo(oldData)==0){
					br.close();
					return s[3];
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	/**
	 *Checks if a new data value exists for a given old data value, for a given columnNumber of a clinical raw data file
	 */	
	public static String getNewDataValue(File wmf, File rawFile, int columnNumber, String oldData){
		if(wmf==null) return null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(wmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(rawFile.getName())==0 && Integer.parseInt(s[1])==columnNumber && s[2].compareTo(oldData)==0){
					br.close();
					return s[3];
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}
	/**
	 *Returns a vector with all terms of a clinical raw data file for a given header
	 */	
	public static Vector<String> getTerms(File rawFile, String header){
		Vector<String> terms=new Vector<String>();
		try{
			int columnNumber=FileHandler.getHeaderNumber(rawFile, header);
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(!terms.contains(s[columnNumber-1])){
					terms.add(s[columnNumber-1]);
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return terms;
	}
	/**
	 *Returns a vector with all terms of a clinical raw data file for a given column number
	 */	
	public static Vector<String> getTermsByNumber(File rawFile, int columnNumber){
		Vector<String> terms=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(!terms.contains(s[columnNumber-1])){
					terms.add(s[columnNumber-1]);
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return terms;
	}
	/**
	 *Returns a vector of data labels from a column mapping file
	 */	
	public static Vector<String> getDataLabels(File cmf, Vector<File> rawFiles){
		Vector<String> dataLabels=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();

			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(!FileHandler.isReserved(s[3])){
				//if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("SUBJ_ID")!=0){
					if(s[3].compareTo("")!=0){
						dataLabels.add(s[3]);
					}
					else{
						File rawFile=null;
						for(File f: rawFiles){
							if(f.getName().compareTo(s[0])==0){
								rawFile=f;
							}
						}
						dataLabels.add(FileHandler.getColumnByNumber(rawFile, Integer.parseInt(s[2])));
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return dataLabels;
	}

	/**
	 *Returns a vector with all data labels, and if there is a data label source a string with "DATA_LABEL:<data_label_source>"
	 */	
		public static Vector<String> getDataLabelsForQC(File cmf, Vector<File> rawFiles){
			Vector<String> dataLabels=new Vector<String>();
			try{
				BufferedReader br = new BufferedReader(new FileReader(cmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(!FileHandler.isReserved(s[3]) && s[3].compareTo("UNITS")!=0){
					//if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("SUBJ_ID")!=0){
						if(s[3].compareTo("DATA_LABEL")==0);
						else if(s[3].compareTo("\\")==0){
							dataLabels.add("DATA_LABEL:"+s[0]+":"+s[4]);
						}
						else if(s[3].compareTo("")!=0){
							dataLabels.add(s[3]);
						}
						else{
							File rawFile=null;
							for(File f: rawFiles){
								if(f.getName().compareTo(s[0])==0){
									rawFile=f;
								}
							}
							dataLabels.add(FileHandler.getColumnByNumber(rawFile, Integer.parseInt(s[2])));
						}
					}
				}
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
			return dataLabels;
		}

	/**
	 *Returns a value for a given subject and a given data label
	 */	
	public static String getValueForSubject(File cmf, Vector<File> rawFiles, String subjectId, String dataLabel, File wmf){
		int columnNumber=-1;
		File rawFile=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[3].compareTo(dataLabel)==0){
					columnNumber=Integer.parseInt(s[2]);
					for(File file: rawFiles){
						if(file.getName().compareTo(s[0])==0){
							rawFile=file;
						}
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
		for(File f: rawFiles){
			if(columnNumber==-1){
				columnNumber=FileHandler.getHeaderNumber(f, dataLabel);
				rawFile=f;
			}
		}
		if(rawFile!=null){
			int subjIdNumber=FileHandler.getNumberForLabel(cmf, "SUBJ_ID", rawFile);
			try{
				BufferedReader br = new BufferedReader(new FileReader(rawFile));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(s[subjIdNumber-1].compareTo(subjectId)==0){
						if(wmf==null){
							br.close();
							return s[columnNumber-1];
						}
						else{
							try{
								BufferedReader br2 = new BufferedReader(new FileReader(wmf));
								String line2=br2.readLine();
								while ((line2=br2.readLine())!=null){
									String[] s2=line2.split("\t", -1);
									if(s2[0].compareTo(rawFile.getName())==0 && s2[1].compareTo(String.valueOf(columnNumber))==0 && s2[2].compareTo(s[columnNumber-1])==0){
										br.close();
										br2.close();
										return  s2[3];
									}
								}
								br2.close();
								return s[columnNumber-1];
							}catch (Exception e){
								e.printStackTrace();
								br.close();
								return "";
							}
						}
					}
				}
				br.close();
			}catch (Exception e){
				e.printStackTrace();
				return "";
			}
		}
		return "";
	}
	
	/**
	 *Returns the last element of the category code for a given column number and clinical raw file name
	 */	
	public static String getValueForSubjectByColumn(File cmf, Vector<File> rawFiles, String subjectId, String columnNumber, String rawFileName, File wmf){
		File rawFile=null;
		for(File file: rawFiles){
			if(file.getName().compareTo(rawFileName)==0){
				rawFile=file;
			}
		}
		if(rawFile==null) return null;
		int column=-1;
		try{
			column=Integer.parseInt(columnNumber);
		}
		catch(NumberFormatException e){
			return null;
		}
		String value=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(subjectId)==0){
					value=s[column-1];
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		if(value==null) return null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(wmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(rawFileName)==0 && s[1].compareTo(columnNumber)==0 && s[2].compareTo(value)==0){
					value=s[3];
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return value;
	}
	
	/**
	 *Returns a value for a given subject and a given data label, taking in count the "DATA_LABEL" labels
	 */	
	public static String getValueForSubjectForQC(File cmf, Vector<File> rawFiles, String subjectId, String dataLabel, File wmf){
		int columnNumber=-1;
		File rawFile=null;
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(dataLabel.split(":", -1).length==1){
					if(s[3].compareTo(dataLabel)==0){
						columnNumber=Integer.parseInt(s[2]);
						for(File file: rawFiles){
							if(file.getName().compareTo(s[0])==0){
								rawFile=file;
							}
						}
					}
				}
				else if(dataLabel.split(":", -1).length==3){
					if(s[3].compareTo("\\")==0 && s[0].compareTo(dataLabel.split(":", -1)[1])==0 && s[4].compareTo(dataLabel.split(":", -1)[2])==0){
						columnNumber=Integer.parseInt(s[2]);
						for(File file: rawFiles){
							if(file.getName().compareTo(s[0])==0){
								rawFile=file;
							}
						}
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return "";
		}
		for(File f: rawFiles){
			if(columnNumber==-1){
				columnNumber=FileHandler.getHeaderNumber(f, dataLabel);
				rawFile=f;
			}
		}
		if(rawFile!=null){
			int subjIdNumber=FileHandler.getNumberForLabel(cmf, "SUBJ_ID", rawFile);
			try{
				BufferedReader br = new BufferedReader(new FileReader(rawFile));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(s[subjIdNumber-1].compareTo(subjectId)==0){
						if(wmf==null){
							br.close();
							return s[columnNumber-1];
						}
						else{
							try{
								BufferedReader br2 = new BufferedReader(new FileReader(wmf));
								String line2=br2.readLine();
								while ((line2=br2.readLine())!=null){
									String[] s2=line2.split("\t", -1);
									if(s2[0].compareTo(rawFile.getName())==0 && s2[1].compareTo(String.valueOf(columnNumber))==0 && s2[2].compareTo(s[columnNumber-1])==0){
										br.close();
										br2.close();
										return  s2[3];
									}
								}
								br2.close();
								return s[columnNumber-1];
							}catch (Exception e){
								e.printStackTrace();
								br.close();
								return "";
							}
						}
					}
				}
				br.close();
			}catch (Exception e){
				e.printStackTrace();
				return "";
			}
		}
		return "";
	}
	
	
	/**
	 *Returns a vector of sample identifiers from gene expression raw data file
	 */	
	public static Vector<String> getSamplesId(File geneFile){
		Vector<String> samples=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(geneFile));
			String line=br.readLine();
			String[] s=line.split("\t", -1);
			for(int i=1; i<s.length; i++){
				samples.add(s[i]);
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}
		return samples;
	}
	
	/**
	 *Checks that a subject to sample mapping file contains subject identifiers
	 */	
	public static boolean checkSubjId(File stsmf){
		try{
			BufferedReader br = new BufferedReader(new FileReader(stsmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[2].compareTo("")==0){
					br.close();
					return false;
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 *Checks that a subject to sample mapping file contains platform
	 */	
	public static boolean checkPlatform(File stsmf){
		try{
			BufferedReader br = new BufferedReader(new FileReader(stsmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[4].compareTo("")==0){
					br.close();
					return false;
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 *Checks that a subject to sample mapping file contains category codes
	 */	
	public static boolean checkCategoryCodes(File stsmf){
		try{
			BufferedReader br = new BufferedReader(new FileReader(stsmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[8].compareTo("")==0){
					br.close();
					return false;
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 *Returns the probe identifier from a gene raw data file
	 */	
	public static Vector<String> getProbes(File rawFile){
		Vector<String> probes=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				probes.add(s[0]);
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}	
		return probes;
	}

	/**
	 *Returns the intensity value for a given sample and a given probe in a gene raw data file
	 */	
	public static Double getIntensity(File rawFile, String sample, String probe){
		try{
			BufferedReader br = new BufferedReader(new FileReader(rawFile));
			String line=br.readLine();
			String[] samples=line.split("\t", -1);
			int columnNumber=-1;
			for(int i=1; i<samples.length; i++){
				if(samples[i].compareTo(sample)==0){
					columnNumber=i;
				}
			}
			if(columnNumber==-1) {
				br.close();
				return null;
			}
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[0].compareTo(probe)==0){
					br.close();
					return Double.valueOf(s[columnNumber]);
				}
			}
			br.close();
			return null;
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}	
	}
	/**
	 *Returns a vector of vectors with for each unit line, the file name, the column number and the data label source
	 *Since version 1.2
	 */	
	public static Vector<Vector<String>> getUnitsLines(File cmf){
		Vector<Vector<String>> v=new Vector<Vector<String>>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[3].compareTo("UNITS")==0){
					Vector<String> vectorLine=new Vector<String>();
					vectorLine.add(s[0]);
					vectorLine.add(s[2]);
					vectorLine.add(s[4]);
					v.add(vectorLine);
				}
			}
			br.close();
			return v;
		}catch (Exception e){
			e.printStackTrace();
			return null;
		}	
	}
	/**
	 *Checks that there are properties in the study tree for clinical data
	 *Since version 1.2
	 */	
	static public boolean checkTreeSet(File cmf){
		boolean bool=false;
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				if(line.compareTo("")!=0){
					String[] s=line.split("\t", -1);
					if(s[1].compareTo("")!=0){
						bool=true;
					}
				}
			}
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}	
		return bool;
	}
	/**
	 *Checks that all terms of a column (replaced if there is a word mapping file) are numerical
	 *Since version 1.2
	 */	
	static public boolean isColumnNumerical(File rawFile, File wmf, int columnNumber){
		Vector<String> terms=FileHandler.getTermsByNumber(rawFile, columnNumber);
		for(String term: terms){
			String newTerm=FileHandler.getNewDataValue(wmf, rawFile, columnNumber, term);
			if(newTerm==null) newTerm=term;
			try{
				if(newTerm.compareTo(".")!=0){
					Double.parseDouble(newTerm);
				}
			}
			catch(NumberFormatException e){
				return false;
			}
		}
		return true;
	}
}
