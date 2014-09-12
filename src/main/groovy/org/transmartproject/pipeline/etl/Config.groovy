// sample configuration file
// replace DB parameters and dataDir with the actual values

db.hostname = 'localhost'
db.port = 1521
db.sid = 'xe'
db.username = 'tm_cz'
db.password = 'tm_cz'

// normally, you don't need to change these

db.jdbcConnectionString = "jdbc:oracle:thin:@${db.hostname}:${db.port}:${db.sid}"
db.jdbcDriver = 'oracle.jdbc.driver.OracleDriver'

// The following specifies a directory containing studies
// It should have the proper data structure, for instance:
// YOUR_ETL_DIRECTORY
//	Public Studies
//		Multiple Sclerosis_Baranzini_GSE13732
//			ClinicalData
//			ExpressionData
//		Multiple Sclerosis_Goertsches_GSE24427
//			ClinicalDataToUpload
//			ExpressionDataToUpload
// As of 0.8 and higher, folders can be nested, e.g: \Public Studies\Test\Multiple Sclerosis_Baranzini_GSE13732

dataDir = 'C:\\tranSMART\\EtlSampleDat1'

// Do not rename if failed (-n option)
// isNoRenameOnFail = true

// Override default ETL behavior and put VISIT NAME prior to data value (--visit-name-first option)
// If you set this, you may still use --data-value-first to override
// visitNameFirst = true
