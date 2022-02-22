// COMMAND ----------

// Databricks notebook source
// DBTITLE 1,Gen 2 Connection String
val keyAdls = "XXXXX"
val credentialAdls = "XXXXXX"
dbutils.widgets.text("EnvType","")
var envType =  dbutils.widgets.get("EnvType")
println(envType)
val envname=envType

var databricksScope : String = _
var adls_storageacc : String = _
var envfoldername : String = _

databricksScope = "XXXXX-KeyVault-Secrets"
adls_storageacc = "XXXXX"
envfoldername = "PROD"

val adlsLoginUrl = "https://login.microsoftonline.com/XXXXXXXXXXXXXXXXXXXXXXXXXXX/oauth2/token"
val decryptedADLSId = dbutils.secrets.get(scope = databricksScope, key = keyAdls)
val decryptedADLSCredential = dbutils.secrets.get(scope = databricksScope, key = credentialAdls)

// Initializing the spark session with the config
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", s"${decryptedADLSId}")
spark.conf.set("dfs.adls.oauth2.credential", s"${decryptedADLSCredential}")
spark.conf.set("dfs.adls.oauth2.refresh.url", adlsLoginUrl)
spark.conf.set("spark.databricks.delta.preview.enabled", "true")
spark.conf.set("spark.databricks.delta.merge.joinBasedMerge.enabled", "true")

// Gen-2
spark.conf.set(s"fs.azure.account.auth.type.$adls_storageacc.dfs.core.windows.net", "OAuth")
spark.conf.set(s"fs.azure.account.oauth.provider.type.$adls_storageacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(s"fs.azure.account.oauth2.client.id.$adls_storageacc.dfs.core.windows.net", s"${decryptedADLSId}")
spark.conf.set(s"fs.azure.account.oauth2.client.secret.$adls_storageacc.dfs.core.windows.net", s"${decryptedADLSCredential}")
spark.conf.set(s"fs.azure.account.oauth2.client.endpoint.$adls_storageacc.dfs.core.windows.net", "https://login.microsoftonline.com/XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX/oauth2/token")
spark.conf.set(s"fs.azure.createRemoteFileSystemDuringInitialization", "true")
spark.conf.set("spark.databricks.io.cache.enabled", "true")


// COMMAND ----------

def insertcomparativetestresult(TestId:String ,Domain:String ,TestName:String ,SourceLayer:String,DestinationLayer:String,Difference:String , Status:String ): String  = 
{
  try
  { 
     if (Status == "Passed")
    {
      val passresult = s"select TestId,Domain,TestName,SourceLayer,DestinationLayer, "+
      s"'${Difference}' as Difference "+
      s",'${Status}' as Status,0 as FailedCounter "+
      "from computestore.ComparativeTestCases where "+
      "lower(TestId) = lower("+ s"'${TestId}') and "+
      "lower(TestName) = lower("+ s"'${TestName}') and lower(Domain) = lower("+ s"'${Domain}') "
      val passresult_spark = spark.sql(passresult)
      passresult_spark.createOrReplaceTempView("passresults_vw")
      val result = s"MERGE into o2cFacts.ComparativeTestResults AS Tgt "+
        	"USING passresults_vw AS Src "+
        	"ON COALESCE(Tgt.TestId,'') = COALESCE(Src.TestID,'') "+
            "AND COALESCE(Tgt.TestName,'') = COALESCE(Src.TestName,'') "+
            "AND COALESCE(Tgt.Domain,'') = COALESCE(Src.Domain,'') "+
        	"WHEN MATCHED "+
            "THEN "+
            "UPDATE SET "+
            "tgt.Difference = Src.Difference,"+
            "tgt.Status = Src.Status, "+
            "tgt.FailedCounter = Src.FailedCounter "+
            "WHEN NOT MATCHED "+
            "THEN INSERT ( "+
            "TestId "+
            ",TestName "+
            ",Domain "+
            ",SourceLayer "+
            ",DestinationLayer "+
            ",Difference "+
            ",FailedCounter "+
            ",Status "+
            ") "+
            "VALUES  "+
            "( "+
            " Src.TestId "+
            ",Src.TestName "+
            ",Src.Domain "+
            ",Src.SourceLayer "+
            ",Src.DestinationLayer "+
            ",Src.Difference "+
            ",Src.FailedCounter "+
            ",Src.Status "+
            ") "
     spark.sql(result)
     println("Insert Successful")
     return "True"
    }
    else
    {
      val failresult = s"select A.TestId,A.Domain,A.TestName,A.SourceLayer,A.DestinationLayer, "+
      s"'${Difference}' as Difference "+
      s",'${Status}' as Status "+
      " ,CASE WHEN B.FailedCounter>0 THEN B.FailedCounter +1 ELSE 1 END as FailedCounter "+
      "from computestore.ComparativeTestCases A "+
      "left join o2cFacts.ComparativeTestResults B on lower(A.TestID) = lower(B.TestId) "+
      "and lower(A.TestName) = lower(B.TestName) and lower(A.domain) = lower(B.domain) "+
      "where lower(A.Domain) = lower("+ s"'${Domain}')  and lower(A.TestId) = lower("+ s"'${TestId}') and lower(A.TestName) = lower("+ s"'${TestName}') "
      val failresult_spark = spark.sql(failresult)
      failresult_spark.createOrReplaceTempView("failresults_vw")
      val result = s"MERGE into o2cFacts.ComparativeTestResults AS Tgt "+
        	"USING failresults_vw AS Src "+
        	"ON COALESCE(Tgt.TestId,'') = COALESCE(Src.TestID,'') "+
            "AND COALESCE(Tgt.TestName,'') = COALESCE(Src.TestName,'') "+
            "AND COALESCE(Tgt.Domain,'') = COALESCE(Src.Domain,'') "+
        	"WHEN MATCHED "+
            "THEN "+
            "UPDATE SET "+
            "Tgt.Difference = Src.Difference, "+
            "Tgt.Status = Src.Status, "+
            "Tgt.FailedCounter = Src.FailedCounter "+
            "WHEN NOT MATCHED "+
            "THEN INSERT ( "+
            "TestId "+
            ",TestName "+
            ",Domain "+
            ",SourceLayer "+
            ",DestinationLayer "+
            ",Difference "+
            ",FailedCounter "+
            ",Status "+
            ") "+
            "VALUES  "+
            "( "+
            " Src.TestId "+
            ",Src.TestName "+
            ",Src.Domain "+
            ",Src.SourceLayer "+
            ",Src.DestinationLayer "+
            ",Src.Difference "+
            ",Src.FailedCounter "+
            ",Src.Status "+
            ") "
     spark.sql(result)
     println("Insert Successful")
     return "True"
    }    
  }
  catch 
  {
    //log the error
    case e: Exception =>
    println("Insert Functional Test Case Failed")
    println("ERROR : Unknown Exception : " + e.getMessage)
    println("Insert Failed")
    throw new Exception("Insert Functional Test Case Failed")
    return "False"
  }
  
}

// COMMAND ----------

def insertfunctionaltestresult(TestId:String ,Domain:String ,TestName:String ,ExpectedResult:String ,ActualResult:String, Status:String ): String  = 
{
  try
  { 
     if (Status == "Passed")
    {
      val passresult = s"select TestId,Domain,TestName,TestQuery,ExpectedResult, "+
      s"'${ActualResult}' as ActualResult "+
      s",'${Status}' as Status,0 as FailedCounter "+
      "from computestore.FunctionalTestCases where "+
      "lower(TestId) = lower("+ s"'${TestId}') and "+
      "lower(TestName) = lower("+ s"'${TestName}') and lower(Domain) = lower("+ s"'${Domain}') "
      val passresult_spark = spark.sql(passresult)
      passresult_spark.createOrReplaceTempView("passresults_vw")
      val result = s"MERGE into o2cFacts.FunctionalTestResults AS Tgt "+
        	"USING passresults_vw AS Src "+
        	"ON COALESCE(Tgt.TestId,'') = COALESCE(Src.TestID,'') "+
            "AND COALESCE(Tgt.TestName,'') = COALESCE(Src.TestName,'') "+
            "AND COALESCE(Tgt.Domain,'') = COALESCE(Src.Domain,'') "+
        	"WHEN MATCHED "+
            "THEN "+
            "UPDATE SET "+
            "tgt.ActualResult = Src.ActualResult,"+
            "tgt.Status = Src.Status, "+
            "tgt.FailedCounter = Src.FailedCounter "+
            "WHEN NOT MATCHED "+
            "THEN INSERT ( "+
            "TestId "+
            ",TestName "+
            ",Domain "+
            ",ExpectedResult "+
            ",ActualResult "+
            ",FailedCounter "+
            ",Status "+
            ") "+
            "VALUES  "+
            "( "+
            " Src.TestId "+
            ",Src.TestName "+
            ",Src.Domain "+
            ",Src.ExpectedResult "+
            ",Src.ActualResult "+
            ",Src.FailedCounter "+
            ",Src.Status "+
            ") "
     spark.sql(result)
     println("Insert Successful")
     return "True"
    }
    else
    {
      val failresult = s"select A.TestId,A.Domain,A.TestName,A.TestQuery,A.ExpectedResult, "+
      s"'${ActualResult}' as ActualResult "+
      s",'${Status}' as Status "+
      " ,CASE WHEN B.FailedCounter>0 THEN B.FailedCounter +1 ELSE 1 END as FailedCounter "+
      "from computestore.FunctionalTestCases A "+
      "left join o2cFacts.FunctionalTestResults B on lower(A.TestID) = lower(B.TestId) "+
      "and lower(A.TestName) = lower(B.TestName) and lower(A.domain) = lower(B.domain) "+
      "where lower(A.Domain) = lower("+ s"'${Domain}')  and lower(A.TestId) = lower("+ s"'${TestId}') and lower(A.TestName) = lower("+ s"'${TestName}') "
      val failresult_spark = spark.sql(failresult)
      failresult_spark.createOrReplaceTempView("failresults_vw")
      val result = s"MERGE into o2cFacts.FunctionalTestResults AS Tgt "+
        	"USING failresults_vw AS Src "+
        	"ON COALESCE(Tgt.TestId,'') = COALESCE(Src.TestID,'') "+
            "AND COALESCE(Tgt.TestName,'') = COALESCE(Src.TestName,'') "+
            "AND COALESCE(Tgt.Domain,'') = COALESCE(Src.Domain,'') "+
        	"WHEN MATCHED "+
            "THEN "+
            "UPDATE SET "+
            "tgt.ActualResult = src.ActualResult, "+
            "tgt.Status = src.Status, "+
            "tgt.FailedCounter = src.FailedCounter "+
            "WHEN NOT MATCHED "+
            "THEN INSERT ( "+
            "TestId "+
            ",TestName "+
            ",Domain "+
            ",ExpectedResult "+
            ",ActualResult "+
            ",FailedCounter "+
            ",Status "+
            ") "+
            "VALUES  "+
            "( "+
            " Src.TestId "+
            ",Src.TestName "+
            ",Src.Domain "+
            ",Src.ExpectedResult "+
            ",Src.ActualResult "+
            ",Src.FailedCounter "+
            ",Src.Status "+
            ") "
     spark.sql(result)
     println("Insert Successful")
     return "True"
    }    
  }
  catch 
  {
    //log the error
    case e: Exception =>
    println("Insert Functional Test Case Failed")
    println("ERROR : Unknown Exception : " + e.getMessage)
    println("Insert Failed")
    throw new Exception("Insert Functional Test Case Failed")
    return "False"
  }
  
}

// COMMAND ----------

def filebaseddataframes (FilePath:String ) : String  = 
{
 var lakepath = "abfss://adlsstore@"+adls_storageacc+".dfs.core.windows.net/"
 var dirpath = lakepath + FilePath  
 //list all the files from the filepath in ADLS Gen2
 try
 {
 val dirList = dbutils.fs.ls(dirpath)  
 //For each file
  println("Start of DataFrame Creation from Files")
  for (dir <- dirList) 
     {
       //Only consider CSV/TSV File 
       if (dir.name.contains(".tsv") || dir.name.contains(".csv"))
       {
           try
             {
               //Remove the ".tsv"/".csv" to get filename
               val filename = dir.name.replace(".tsv","").replace(".csv","")
               val filepath = dirpath +dir.name
               println(dir.name)
               //generate the dataframe for each CSV/TSV File
               val df1 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","true").load(filepath)
               df1.createOrReplaceTempView(filename)
             }
            catch 
             {
               //log the error
               case e: Exception =>
               println("Exception Occurred during reading CSV/TSV File")
               println("ERROR : Unknown Exception : " + e.getMessage)
             }
       }
     }
   return "True"
}
catch 
   {
     //log the error
     case e: Exception =>
     println("Exception Occurred during reading CSV/TSV File")
     println("ERROR : Unknown Exception : " + e.getMessage)
     return "False"
   }
}

// COMMAND ----------

// DBTITLE 1,Functional Testing
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
//get the widget text for area
dbutils.widgets.text("domain", "dv")
val domain = dbutils.widgets.get("domain")
//truncate the results table
//val TruncateFuncationalTestCases = """ TRUNCATE TABLE o2cFacts.FunctionalTestResults """
//spark.sql(TruncateFuncationalTestCases)
//println("Truncation Table Succeeded")
//get the test cases from the config table
val BVTTestCases = s"select TestId,Domain,TestName,TestQuery,ExpectedResult,FilePath from computestore.FunctionalTestCases where lower(Domain) = lower("+ s"'${domain}') and IsActive = '1' "
val BVTTestCases_Finalquery = spark.sql(BVTTestCases)
println("Config Table Read Succeeded")
var filelist = ListBuffer[String]()
//for each test case in config execute the loop

for (row <- BVTTestCases_Finalquery.rdd.collect)
{ 
breakable { 
    //get the columns for the test case
    val TestId = row.getString(0)
    val Domain = row.getString(1)
    val TestName = row.getString(2)  
    val TestQuery = row.getString(3) 
    val ExpectedResult = row.getString(4)
    val FilePath = row.getString(5)
    //println(row.getString(3))
    if(FilePath.contains("""/"""))
        {
          if (filelist.contains(FilePath))
          {
            println("Dataframes already exists")
          }
          else 
          {
          val output = filebaseddataframes (FilePath)
          if (output == "False")
            {
             insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,"", "Exception while File load") 
             break
            }
          else 
            {
             filelist += FilePath
            }  

          }
        }
    try 
    {
        val BVTTestCaseResult = spark.sql(TestQuery)
        if (BVTTestCaseResult.count()>=1)
           {
             val ActualResult =BVTTestCaseResult.head(10).mkString(" ")
             if (ActualResult.trim().toLowerCase ()  == ExpectedResult.trim().toLowerCase () )
             {
              val Status = "Passed"
              println("Start of Functional Test Case Successful Result Entry")
              insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,ActualResult, Status)
              println("End of Functional Test Case Successful Result Entry")
             }
             else 
             {
               val Status = "Failed"
               println("Start of Functional Test Case Failed Result Entry")
               insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,ActualResult, Status)
               println("End of Functional Test Case Failed Result Entry")
              }
           }
        else
           {
             val ActualResult ="null"
             if (ActualResult.trim().toLowerCase ()  == ExpectedResult.trim().toLowerCase ())
             {
              val Status = "Passed"
              println("Start of Functional Test Case Successful Result Entry")
              insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,ActualResult, Status)
              println("End of Functional Test Case Successful Result Entry")
             }
             else 
             {
               val Status = "Failed"
               println("Start of Delta Table based Test Case Failed Result Entry")
               insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,ActualResult, Status)
               println("End of Functional Test Case Failed Result Entry")
              }
           }
        
      }
    catch 
    {
       case e: Exception =>
       println("Exception Occurred during merging FunctionalTestResults Table")
       println("ERROR : Unknown Exception : " + e.getMessage)
       val Status = "Test Case Definition Incorrect"
       println("Start of Exception Result Entry")
       insertfunctionaltestresult(TestId,Domain,TestName,ExpectedResult,"", Status)
       println("End of Exception Result Entry")
    }
}    
}

// COMMAND ----------

// DBTITLE 1,Comparative Test Cases
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
//dbutils.widgets.text("domain", "dv")
val domain = dbutils.widgets.get("domain")
//val TruncateComparativeTestCases = """ TRUNCATE TABLE o2cFacts.ComparativeTestResults """
//spark.sql(TruncateComparativeTestCases)
//println("Truncation Table Succeeded")
val ComparativeTestCases = s"select TestID,Domain,TestName,SourceLayer,SourceQuery,DestinationLayer,DestinationQuery from computestore.ComparativeTestCases where lower(Domain) = lower("+ s"'${domain}') and IsActive = '1' "
val ComparitiveTestCases_Finalquery = spark.sql(ComparativeTestCases)
println("Config Table Read Succeeded")
var filelist = ListBuffer[String]()
for (row <- ComparitiveTestCases_Finalquery.rdd.collect)
{ 
breakable { 
    val TestId = row.getString(0)
    val Domain = row.getString(1)
    val TestName = row.getString(2)  
    val SourceLayer = row.getString(3) 
    val SourceQuery = row.getString(4)
    val DestinationLayer = row.getString(5)  
    val DestinationQuery = row.getString(6) 
    //Load the dataframes for file based sources
    if(SourceLayer.contains("""/"""))
        {
          if (filelist.contains(SourceLayer))
          {
            println("Dataframes already exists")
          }
          else 
          {
          val output =filebaseddataframes (SourceLayer)
            if (output == "False")
            {
              insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , "Exception while File load" )  
              break
            }
            else 
            {
             filelist += SourceLayer
            } 
          }
        }
    else if(DestinationLayer.contains("""/"""))
        {
          if (filelist.contains(DestinationLayer))
          {
            println("Dataframes already exists")
          }
          else 
          {
          val output = filebaseddataframes (DestinationLayer)
          if (output == "False")
            {
              insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , "Exception while File load" )  
              break
            }
            else 
            {
             filelist += DestinationLayer
            } 
          
          }
        }
  //Run the Comparative Test Cases  
  try
       {        
        val Result1 = spark.sql(SourceQuery)
        val Result2 = spark.sql(DestinationQuery)
       //Check if schema is not matching
        //if (!Result1.schema.toString().equalsIgnoreCase(Result2.schema.toString))
         // {
         //   val Status = "Failed"
         //    println("Failed")
         //    insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , Status )       
         //  }      
         //Check : If the output is having 1 row only
         if (Result1.count()==1  && Result2.count()==1)
                 {
                   val diff = Result1.except(Result2)
                   val diffright = Result2.except(Result1)
                   val Difference = diff.union(diffright).head(2).mkString(" ")
                   if (diff.count()==1)
                      {
                        val Status = "Failed"
                        println("Failed")
                        insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,Difference, Status )   
                      }
                   else
                      {
                        val Status = "Passed"
                        println("Passed")
                        insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , Status )   
                      }
                }
         //Check : If the output is having multiple rows
         else if (Result1.count()>1 || Result2.count()>1)
           {
             val diffleft = Result1.except(Result2)
             val diffright = Result2.except(Result1)
             val diff = diffleft.union(diffright)
             val Difference = diffleft.union(diffright).head(10).mkString(" ") 
             if (diff.count()>=1)
             {
               val Status = "Failed"
               println("Failed")
               insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,Difference , Status ) 
    
             }
             else
             {
               val Status = "Passed"
               println("Passed")
               insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , Status )  
             }
           }
        }
    catch 
       {
         case e: Exception =>
         println("Exception Occurred")
         println("ERROR : Unknown Exception : " + e.getMessage)
         val error = e.getMessage
         val Status = "Exception"
         println("Exception")
         insertcomparativetestresult(TestId ,Domain ,TestName ,SourceLayer,DestinationLayer,"" , Status ) 
       }      
}
}

// COMMAND ----------

// DBTITLE 1,Views For Notification
val SrcRawMismatch = """ select * from o2cFacts.SourceTargetMismatch WHERE CAST((Replace(RelativeDiff,'%','')) AS Decimal(7,2)) >= 5.0   """
val MST = spark.sql(SrcRawMismatch)
MST.createOrReplaceTempView("MST_view")

val FunctionalTests = """ SELECT A.TestId, A.Domain, A.TestName, A.ExpectedResult, A.ActualResult, A.Status, A.FailedCounter,
  CASE WHEN FailedCounter = 2 THEN 'Medium'
  WHEN FailedCounter = 3 THEN 'High' 
  WHEN FailedCounter > 3 THEN 'Critical' 
  ELSE 'Low' END as Severity
FROM o2cFacts.FunctionalTestResults A
JOIN computestore.FunctionalTestCases B ON A.TestId = B.TestID and A.TestName = B.TestName
WHERE lower(Status) <> 'passed' and IsActive = '1'
ORDER BY FailedCounter DESC """
val FT = spark.sql(FunctionalTests)
FT.createOrReplaceTempView("FT_view")

val ComparativeTests = """ SELECT A.TestId, A.Domain, A.TestName, A.SourceLayer, A.DestinationLayer, A.Difference, A.Status, A.FailedCounter,
  CASE WHEN FailedCounter = 2 THEN 'Medium'
  WHEN FailedCounter = 3 THEN 'High' 
  WHEN FailedCounter > 3 THEN 'Critical' 
  ELSE 'Low' END as Severity
FROM o2cFacts.ComparativeTestResults A
JOIN computestore.ComparativeTestCases B ON A.TestId = B.TestID and A.TestName = B.TestName
WHERE lower(Status) <> 'passed' and IsActive = '1'
ORDER BY FailedCounter DESC """
val CT = spark.sql(ComparativeTests)
CT.createOrReplaceTempView("CT_view")

// COMMAND ----------

// MAGIC %python
// MAGIC def highlight_vals (val):
// MAGIC   if(val == 'Medium'):
// MAGIC     return 'background-color: %s' % '#ffff29'
// MAGIC   elif(val == 'High'):
// MAGIC     return 'background-color: %s' % '#ffab0f'
// MAGIC   elif(val == 'Critical'):
// MAGIC     return 'background-color: %s' % '#f50000'
// MAGIC   elif(val == 'Low'):
// MAGIC     return 'background-color: %s' % '#00eb00'
// MAGIC   else:
// MAGIC     return ''

// COMMAND ----------

// DBTITLE 1,HTML Mail Body for Notification
// MAGIC %python
// MAGIC Source_RawMismatch_spark = spark.table("MST_view")
// MAGIC Source_RawMismatch = Source_RawMismatch_spark.toPandas()
// MAGIC 
// MAGIC if(len(Source_RawMismatch.index) == 0):
// MAGIC   HTMLMismatchHeading = ""
// MAGIC   MismatchResult = ""
// MAGIC else:
// MAGIC   HTMLMismatchHeading = """
// MAGIC   <br>
// MAGIC   <h3>Records with Variance between Source and Raw Layers > 5% </h3>
// MAGIC   """
// MAGIC   MismatchResult = Source_RawMismatch.to_html()
// MAGIC   
// MAGIC 
// MAGIC FunctionalTest_spark = spark.table("FT_view")
// MAGIC FunctionalTest = FunctionalTest_spark.toPandas()
// MAGIC 
// MAGIC if(len(FunctionalTest.index) == 0):
// MAGIC   HTMLFunctionalHeading = ""
// MAGIC   FunctionalResult = ""
// MAGIC else:
// MAGIC   HTMLFunctionalHeading = """
// MAGIC   <br>
// MAGIC   <h3>Records with Failed Functional Test Cases </h3>
// MAGIC   """
// MAGIC   FunctionalResult = FunctionalTest.style.applymap(highlight_vals, subset=['Severity']).set_table_attributes("border=1").render()
// MAGIC 
// MAGIC ComparativeTest_spark = spark.table("CT_view")
// MAGIC ComparativeTest = ComparativeTest_spark.toPandas()
// MAGIC 
// MAGIC if(len(ComparativeTest.index) == 0):
// MAGIC   HTMLComparativeHeading = ""
// MAGIC   ComparativeResult = ""
// MAGIC else:
// MAGIC   HTMLComparativeHeading = """
// MAGIC   <br>
// MAGIC   <h3>Records with Failed Comparative Test Cases </h3>
// MAGIC   """
// MAGIC   ComparativeResult = ComparativeTest.style.applymap(highlight_vals, subset=['Severity']).set_table_attributes("border=1").render()
// MAGIC 
// MAGIC HTMLContentStart = """
// MAGIC <!DOCTYPE html>
// MAGIC <html>
// MAGIC <head>
// MAGIC <style>
// MAGIC table, th, td {
// MAGIC   border: 1px solid black;
// MAGIC   border-collapse: collapse;
// MAGIC }
// MAGIC th {
// MAGIC   text-align: center;
// MAGIC   font-weight: bold;
// MAGIC }
// MAGIC th,td {
// MAGIC   padding: 7px;
// MAGIC }
// MAGIC </style>
// MAGIC </head>
// MAGIC <body>
// MAGIC <h2>Data Validation Results</h2>
// MAGIC """
// MAGIC 
// MAGIC HTMLContentEnd = """
// MAGIC </body>
// MAGIC </html>
// MAGIC """
// MAGIC 
// MAGIC HTMLContentBody = HTMLMismatchHeading + MismatchResult + HTMLFunctionalHeading + FunctionalResult +  HTMLComparativeHeading + ComparativeResult
// MAGIC 
// MAGIC if(HTMLContentBody == ""):
// MAGIC   HTMLMailBody = "<!DOCTYPE html>"
// MAGIC else:
// MAGIC   HTMLMailBody = HTMLContentStart + HTMLContentBody + HTMLContentEnd
// MAGIC dbutils.fs.put("mnt/adlsgen2/folder/sendmail/mail.txt", HTMLMailBody,True)
