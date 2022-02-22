Create Table SUBJECT_AREA_DB.FunctionalTestCases (
TestID string,	
Domain string,	
TestName string,	
TestQuery string,	
ExpectedResult string,  
FilePath string,    
IsActive string
) USING org.apache.spark.sql.json  OPTIONS (path '/Metadata/ExternalTable/FunctionalTestCases.json')