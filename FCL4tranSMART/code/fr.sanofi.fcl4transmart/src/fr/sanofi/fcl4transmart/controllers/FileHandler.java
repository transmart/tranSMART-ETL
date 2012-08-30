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
package fr.sanofi.fcl4transmart.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

public class FileHandler {
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
	//Function reading a column mapping file, and returning the column number for the line where label=string, for a given raw file
	public static int getNumberForLabel(File file, String string, File rawFile){
		try{
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
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
			
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return -1;
	}
	//Function reading a column mapping file, and returning the raw data file name for the line where label=string
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
	//return the header for the n-th column of data file 
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
	//return a vector of string containing all headers of raw data that will be integrated in databases (non omited and not identifiers in cmf)
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
					if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("SUBJ_ID")!=0 && s[3].compareTo("\\")!=0){
						headers.add(rawHeaders[Integer.parseInt(s[2])-1]);
					}
				}
			}
			
			br.close();
		}catch (Exception e){
			e.printStackTrace();
		}
		return headers;
	}
	//return a vector of string containing all headers of raw data that are not omitted)
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
							headers.add(rawHeaders[Integer.parseInt(s[2])-1]);
						}
					}
				}
				
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
			return headers;
		}
	//return data label from a header of raw data
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
						br.close();
						return s[3];
					}
				}
				
				br.close();
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		return "";
	}
	//return controled vocabulary code from header
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
	//check if a new data value exists for a given old data value, for one header of a raw file
	public static String getNewDataValue(File wmf, File rawFile, String header, String oldData){
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
	//return all data labels from cmf
	public static Vector<String> getDataLabels(File cmf, Vector<File> rawFiles){
		Vector<String> dataLabels=new Vector<String>();
		try{
			BufferedReader br = new BufferedReader(new FileReader(cmf));
			String line=br.readLine();
			while ((line=br.readLine())!=null){
				String[] s=line.split("\t", -1);
				if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("SUBJ_ID")!=0){
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
	//return all data labels from cmf, and last path element for "DATA_LABEL" label
		public static Vector<String> getDataLabelsForQC(File cmf, Vector<File> rawFiles){
			Vector<String> dataLabels=new Vector<String>();
			try{
				BufferedReader br = new BufferedReader(new FileReader(cmf));
				String line=br.readLine();
				while ((line=br.readLine())!=null){
					String[] s=line.split("\t", -1);
					if(s[3].compareTo("OMIT")!=0 && s[3].compareTo("VISIT_NAME")!=0 && s[3].compareTo("SITE_ID")!=0 && s[3].compareTo("SUBJ_ID")!=0){
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
	//return a value for a given subject and a given data label
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
	//return the last element of the category code for a given column number and raw file name
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
	//return a value for a given subject and a given data label, taking in count the "DATA_LABEL" labelss
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
	
	//return a vector of sample identifiers from gene expression raw data file
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
	//check that a subject to sample mapping file contains subject identifiers
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
	//check that a subject to sample mapping file contains platform
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
	//check that a subject to sample mapping file contains category codes
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
	//return probe id from a gene raw data file
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
	//return the intensity value for a given sample and a given probe in a gene raw data file
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
}
