CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.ComparativeTestResults (
   TestID  STRING,
   Domain STRING,
   TestName STRING,
   SourceLayer STRING,
   DestinationLayer STRING,
   Difference STRING,
   Status STRING,
   FailedCounter INT)

USING delta OPTIONS (path 'ADLS_PATH_GEN2/ENV_TYPE/XXXXX/XXXXX/XXXXXXX/comparativetestresults/full')