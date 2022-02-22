# Functional Validation Testing Framework in Spark SQL

## Scenario: 
You are working on a data engineering project. You have built the system by applying functional(business/quality) rules and want to ensure, that if a business rule is not confirmed, you would like to report the discrepancy.

The discrepancies could include but not restricted to:
* Source layer and Target layer are not in sync
* Key Columns in a table are empty
* Data Quality issues with a table e.g. duplicates on primary columns causing merge issues etc.
* Business Rules Validation Scenarios e.g. Amount should not be 0 for a region, Key metrics should not vary between months by a threshold etc.

## How Functional Validation Testing Framework in Spark SQL helps you?
The framework tries to classify the test cases into two types:
* Functional Test Cases : Negative testing or testing against a known output.
* Comparitive Test Cases : Writing test cases for each layer(source and destination) and comparing the outputs.

### Functional Test Cases: 
Refer to the (/Functional-Validation-Testing-Spark-SQL/Test Cases Scripts/FunctionalTestCases.json)
Consists of following inputs:
* TestID - ID of a test case. Can be based on user preference.
* Domain - Domain of a test cases. User can classify the test cases based on domains it serve to report domain wise test case coverage easily.
* TestName - User Friendly Description of the Test Case.
* TestQuery - Test Query based on the functional Test cases in Spark SQL.
* ExpectedResult - Expected Value of the functional Test case to classify it as passed.
* FilePath - File path if provided allows all CSV/TSV files to be read up from the mounted lake location and a dataframe created for them for comparision. This will allow the test query to instead run on the dataframe instead of delta table.
* IsActive - In some cases, the test case could become obsolete. This flag can allow to report only active test cases.

### Comparative Test Cases:
Refer to the (/Functional-Validation-Testing-Spark-SQL/Test Cases Scripts/ComparativeTestCases.json)
Consists of following inputs:
* TestID - ID of a test case. Can be based on user preference.
* Domain - Domain of a test cases. User can classify the test cases based on domains it serve to report domain wise test case coverage easily.
* TestName - User Friendly Description of the Test Case.
* Source Layer - User Friendly Layer of Source Layer. Can be a File Path to run against CSV/TSV
* Source Query - Test Query for Source Layer
* Destination Layer - User Friendly Layer of Destination Layer. Can be a File Path to run against CSV/TSV
* Destination Query - Test Query for Destination Layer

## Pre-requisites to Use the Framework
* Databricks
* Cluster with following specifications: Scala 2.11 or higher, Spark 2.4.3 or higher and Environment Variable: PYSPARK_PYTHON=/databricks/python3/bin/python3 
* Logic Apps - For Notification Mechanism - Optional
* Azure Data Factory - For Orchestration - Optional

## Steps to Use
Step 1: Identify Scenarios for Functional and Comparative Testing.
Step 2: Create Test Queries for each of the test scenario.
Step 3: Create json for Functional and Comparative Test Cases based on the sample.
Step 4: Deploy the json to the databricks workspace at "/Metadata/ExternalTable/" or any other location in dbfs
Step 5: Create test cases tables based on "/Functional-Validation-Testing-Spark-SQL/Test Cases Table Creation Scripts/" SQL Scripts
Step 6: Create test results tables based on "Functional-Validation-Testing-Spark-SQL/Results Table Creation Scripts/" SQL Scripts
Step 7: Deploy the FunctionalValidationTestingSparkSQL.scala in your databricks workspace.
Step 8(Optional) : Create an ADF Pipeline to schedule the orchestration of FunctionalValidationTestingSparkSQL.scala notebook.
Step 9(Optional): Create a Logic Apps to trigger the Email/Teams notification based on the html output from FunctionalValidationTestingSparkSQL.scala

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
