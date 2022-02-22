Create Table SUBJECT_AREA_DB.ComparativeTestCases (
TestID string,	
Domain string,	
TestName string,	
SourceLayer string,	
SourceQuery string,	
DestinationLayer string,	
DestinationQuery string,    
IsActive string
) USING org.apache.spark.sql.json  OPTIONS (path '/Metadata/ExternalTable/ComparativeTestCases.json')