import java.io.BufferedReader;
import java.util.Arrays;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

//import java.util.Iterator;
//import java.util.LinkedHashMap;
//import java.util.Map;

public class FilePivot {

	/*
	 * required parameters 
	 * 1)pivotstart - pivot start position 0 based
	 * 2)columncount - number of data columns to be pivoted for each record, think groups of columns 
	 * 3)inputfile - input data file 
	 * 4)outputfile - output data file
	 * 
	 * optional parameters
	 * 1)delimiter - column delimiter . Default is tab (\t)
	 * 2)fixedcolumns - columns from row that will be on every output record, comma separated column numbers (0 based)
						the columns will be placed in the output record in the order that they are specified in the list
						For example "3,5" will have column 3 in column 1 of the output record and column 5 in column 2 of the output record
	 * 3)skipcolumns - comma separated column numbers (0 based) of columns that will not be in the output record. e.g. "6,10, 11"
	 * 4)maxcolumns - maximum number of data columns to pivot from input file
	 * 5)hdrstart - row number of header row (1 based)
	 * 6)nbrrows - number of rows in header, the values in each row will be populated as columns in the output file
	 * 7)dataend - last row of data (1 based)
	 * 8)delimtotab - set delimiter character to tab before any processing
	 */

	public static void main(String[] args) {

		if(args.length < 1){
			System.out.println("Usage: FilePivot  pivotstart=? columncount=? inputfile=? outputfile=? ");
			System.out.println("* required parameters");
			System.out.println("* 1)pivotstart - pivot start position (0 based. All columns to the left will be inserted into each output record");
			System.out.println("* 2)columncount - number of data columns to be pivoted for each record"); 
			System.out.println("* 3)inputfile - input data file"); 
			System.out.println("* 4)outputfile - output data file");
			System.out.println("* optional parameters");
			System.out.println("* 1)delimiter - column delimter. Default is tab (\t)");
			System.out.println("* 2)skipfixedcolumns - comma separated column numbers (0 based). e.g. \"3,5\"");
			System.out.println("* 3)skipdatacolumns - comma separated column numbers (0 based). e.g. \"6\"");
			System.out.println("* 4)maxcolumns - maximum number of data columns to pivot from input file");
			System.out.println("* 5)hdrstart - row number of header row (1 based)"); 
			System.out.println("* 6)nbrrows - number of rows in header, the values in each row will be populated as columns in the output file");
			System.out.println("* 7)dataend - last row of data (1 based)");
			System.out.println("* 8)delimtotab - set delimiter character to tab before any processing");

			return ;
		}
		
		//	define variables for arguments
		
		int apivotStartPosition = -99;
		int anbrColstoPivot = -99;
		int amaxColumns = 99999999;
		int anbrRows = 0;
		int ahdrStart = 0;
		int adataEnd = 99999999;
		String adataFile = null;
		String aoutputFile = null;
		String adelimChar = null;
		String askipStatic = null;
		String askipData = null;
		boolean adelimtoTab = false;
		boolean askipdataAbs = false;
		boolean unkArg = true;

		// loop get args
		for(String s:args)
		{
			if(s!=null)
			{
				String[] tokens = s.split("=");
				unkArg = true;

				//	check for token name
				
				if ((tokens[0].toUpperCase()).equals("PIVOTSTART"))
				{
					try 
					{
						apivotStartPosition = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("COLUMNCOUNT"))
				{
					try 
					{
						anbrColstoPivot = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("INPUTFILE"))
				{
					adataFile = tokens[1];
					unkArg = false;
				}
								
				if ((tokens[0].toUpperCase()).equals("OUTPUTFILE"))
				{
					aoutputFile = tokens[1];
					unkArg = false;
				}
			
				if ((tokens[0].toUpperCase()).equals("DELIMITER"))
				{
					adelimChar = tokens[1];
					unkArg = false;
				}
			
				if ((tokens[0].toUpperCase()).equals("SKIPFIXEDCOLUMNS"))
				{
					askipStatic = tokens[1];
					unkArg = false;
				}
						
				if ((tokens[0].toUpperCase()).equals("SKIPDATACOLUMNS"))
				{
					askipData = tokens[1];
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("DELIMTOTAB"))
				{
					adelimtoTab = true;
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("MAXCOLUMNS"))
				{
					try 
					{
						amaxColumns = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("NBRROWS"))
				{
					try 
					{
						anbrRows = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("HDRSTART"))
				{
					try 
					{
						ahdrStart = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if ((tokens[0].toUpperCase()).equals("DATAEND"))
				{
					try 
					{
						adataEnd = Integer.parseInt(tokens[1]);
					} 
					catch (NumberFormatException e) 
					{
						System.err.println(tokens[0] + " Argument must be an integer");
						System.exit(1);
					}
					unkArg = false;
				}
				
				if (unkArg)
				{
					System.err.println(tokens[0] + " Argument is not valid");
					System.exit(1);
				}
			}
		}
		
		// generate error on required params
			
		if (apivotStartPosition < 0 )
		{
			System.err.println("PivotStart Argument is missing");
			System.exit(1);
		}
		
		if (anbrColstoPivot < 0 )
		{
			System.err.println("ColumnCount Argument is missing");
			System.exit(1);
		}
		
		if (adataFile == null)
		{
			System.err.println("InputFile Argument is missing");
			System.exit(1);
		}
		
		if (aoutputFile == null)
		{
			System.err.println("OutputFile Argument is missing");
			System.exit(1);
		}
		
		// run
		pivotData(apivotStartPosition, anbrColstoPivot, adataFile,
				aoutputFile, adelimChar, askipStatic, askipData, amaxColumns, anbrRows, adelimtoTab, ahdrStart, adataEnd);
	}

	private static void pivotData(int pivotStartPosition, int nbrColstoPivot,
			String dataFile, String outputFile, String delimChar,
			String skipStatic, String skipData, int maxColumns, int nbrRows, boolean delimtoTab
			,int hdrStart, int dataEnd) {
		File mapfile = new File(dataFile);
		String filename = outputFile;
		BufferedReader reader = null;
		BufferedWriter writer = null;

		try {
			reader = new BufferedReader(new FileReader(mapfile));
			writer = new BufferedWriter(new FileWriter(filename));
			String line = "";
			String hdrLine = null;
			String tabChar = "\t";
			String tmpLine = null;
			String[][] lblArray = new String[10][10000];

			int dCount = 0;
			int cCount = 0;
			int tmpInt = 0;
			int rowCount = 0;
			int tmpInt2 = 0;
			int hdrRow = 0;
			boolean needFlds = true;
			boolean booleanFillValue = false;
			boolean[] skipStaticCols = new boolean[50];
			Arrays.fill(skipStaticCols, booleanFillValue);
			boolean[] skipDataCols = new boolean[50];
			Arrays.fill(skipDataCols, booleanFillValue);
			
			
			String forceTab = "*" + delimChar + "*";

			if (delimChar == null) {
				delimChar = "\t";
			}

			if (!(skipStatic == null)) {
				String[] tokens = skipStatic.split(",");

				for (String token : tokens) {
					try {
						// convert the column to int and set the boolean array
						// value to true
						String stoken = token != null ? token.trim() : token;
						tmpInt = Integer.parseInt(stoken);
						if (tmpInt < pivotStartPosition) {
							skipStaticCols[tmpInt] = true;
						} else {
							System.out.println("Static Column to be skipped not less than Pivot Start Position");
							System.exit(16);
						}
					} catch (NumberFormatException nfe) {
						System.out.println("NumberFormatException: "
								+ nfe.getMessage());
						System.exit(16);
					}
				}
			}

			if (!(skipData == null)) {
				String[] tokens = skipData.split(",");

				for (String token : tokens) {
					try {
						// convert the column to int and set the boolean array
						// value to true
						String stoken = token != null ? token.trim() : token;
						tmpInt = Integer.parseInt(stoken);
						if (tmpInt < nbrColstoPivot) {
							skipDataCols[tmpInt] = true;
						} else {
							System.out.println("Data Column to be skipped not less than Number of Cols to Pivot");
							System.exit(16);
						}
					} catch (NumberFormatException nfe) {
						System.out.println("NumberFormatException: "
								+ nfe.getMessage());
						System.exit(16);
					}
				}
			}

			while ((line = reader.readLine()) != null) {
				if (rowCount > nbrRows) {
					
					if (delimtoTab)
					{
						tmpLine = line.replaceAll(forceTab,tabChar);
					}
					else
					{
						tmpLine = line;
					}

					String[] tokens = tmpLine.split(delimChar);
					//System.out.println("nbr tokens: " + tokens.length);
					cCount = 0;
					dCount = 99999; // set to 99999 to force new record
					StringBuilder comColumns = new StringBuilder();
					String colStr = null;
					for (String token : tokens) {
						String stoken = token != null ? token.trim() : token;
						//System.out.println("stoken: " + stoken);
						//System.out.println("cCount: " + cCount);
						//System.out.println("dcount: " + dCount);
						//System.out.println("pivotStartPosition: " + pivotStartPosition);
						if (cCount < pivotStartPosition) {
							if (!(skipStaticCols[cCount])) {
								comColumns.append(stoken).append(tabChar);
								colStr = comColumns.toString();
							}
						} else {
							if (dCount >= nbrColstoPivot) {
								// start new record
								writer.newLine();
								writer.write(colStr);
								for (int la=0; la<nbrRows+1; la++)
								{writer.write(lblArray[la][(cCount - pivotStartPosition)
												/ nbrColstoPivot]);
								writer.write(tabChar);
								}
								dCount = 0;
							}

							if (!(skipDataCols[dCount])) {
								// write subsequent pivot columns
								// writer.write(tabChar);
								writer.write(stoken);
								writer.write(tabChar);
							}

							dCount++;
						}
 
						cCount++;
						//	check if all columns have been processed
						
						if (cCount >= maxColumns)
						{
							break;
						}
					}
					rowCount++;
				} 
				else 
				{

					// pickup headers from first row
					String[] hdrLabels = line.split(delimChar);
					tmpInt = 0;
					cCount = 0;
					boolean firstHdr = true;

					if (rowCount < nbrRows)
					{
						//	get column values for rows before last header row
						
						for (int hx = pivotStartPosition; hx < hdrLabels.length; hx++)
						{
							// only pickup first header of group as data label
							lblArray[rowCount][tmpInt] = hdrLabels[hx];

							hx = hx + nbrColstoPivot - 1; // skip over additional data cols
							tmpInt++;
							cCount++;

							if (cCount >= maxColumns)
							{
								break;
							}
						}
						rowCount++;
					}
					else
					{
						for (int hl = 0; hl < hdrLabels.length; hl++) {

							if (hl < pivotStartPosition) {
								// these are the static headers from the first row of the input file

								if (!(skipStaticCols[hl])) {
									if (firstHdr) {
										hdrLine = hdrLabels[hl];
										firstHdr = false;
									} else {
										hdrLine = hdrLine + tabChar + hdrLabels[hl];
									}
								}
							} else {
								if (needFlds) {
									// create column labels for fields being pivoted
									for (int hf = 0; hf < nbrColstoPivot + nbrRows + 1; hf++) {
										if (!(skipDataCols[hf])) {
											tmpInt2 = tmpInt2 + 1;
											hdrLine = hdrLine + tabChar + "Field "
													+ String.valueOf(tmpInt2);
										}
									}
									needFlds = false;

									// setup to get correct data label value if data
									// fields are skipped
									for (int hx = 0; hx < nbrColstoPivot; hx++) {
										if (!(skipDataCols[hx])) {
											break;
										}
										hl++;
									}
								}

								// only pickup first header of group as data label

								lblArray[rowCount][tmpInt] = hdrLabels[hl];
								hl = hl + nbrColstoPivot - 1; // skip over
															// additional cols
								tmpInt++;
								cCount++;

								if (cCount >= maxColumns)
								{
									break;
								}
							}
						}
						writer.write(hdrLine);
						rowCount++;
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			try {
				if (writer != null) {
					writer.flush();
					writer.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}

		}

	}
}
