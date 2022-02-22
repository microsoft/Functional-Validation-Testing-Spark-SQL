CREATE TABLE IF NOT EXISTS SUBJECT_AREA_DB.FunctionalTestResults (
   TestId  STRING,
   Domain STRING,
   TestName STRING,
   ExpectedResult STRING,
   ActualResult STRING,
   Status STRING,
   FailedCounter INT)
USING delta OPTIONS (path 'ADLS_PATH_GEN2/ENV_TYPE/XXXXXXX/XXXXXXX/XXXXXX/functionaltestresults/full')