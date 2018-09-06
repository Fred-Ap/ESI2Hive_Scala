import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.util.Date

val conf = new SparkConf().setAppName("ESI2HIVE")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val hiveContext = new HiveContext(sc)
hiveContext.setConf("hive.exec.dynamic.partition", "true")
hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
hiveContext.setConf("hive.exec.max.dynamic.partitions.pernode", "400")

val latestTimestampDf = sqlContext.sql("select MAX(eventtimestamp) from esi2hive.odl_osdata_cpu_index")
val latestTimestamp = latestTimestampDf.first()
val latestTimestampValue = latestTimestamp.getTimestamp(0)
println(latestTimestampValue)
var queryMap = collection.mutable.Map[String, String]()


#### Will not run first time ###

if(latestTimestampValue != null){

   val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
   simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
   val maxDate= new Date(latestTimestampValue.getTime() + 120*60*1000)
   val minDate= new Date(latestTimestampValue.getTime() + 1000)
   val maxRange=simpleDateFormat.format(maxDate)
   val minRange=simpleDateFormat.format(minDate)
   println(minRange)
   println(maxRange)
   val es_query = "?q=eventTimestamp:[" + minRange + " TO " + maxRange + "]"
   println(es_query)
   queryMap += ("es.query" -> es_query)

} else{

}
println(queryMap)

val es_df=sqlContext.read.format("org.elasticsearch.spark.sql").options(queryMap).load("odl-osdata-cpu-index")
println(es_df.count())
es_df.show(15)
es_df.printSchema()

val newNames = Seq("inserttimestamp", "appName", "cpuName", "eventtimestamp", "pctiowait", "pctidle", "pctsteal" , "pctsystem" , "pctuser" , "servername" , "unique_id")
val dfRenamed = es_df.toDF(newNames: _*)

dfRenamed.show(15)
dfRenamed.printSchema()

val cleandata=sqlContext.sql("DROP TABLE IF EXISTS esi2hive.odl_osdata_cpu_index_tmp")

dfRenamed.write.saveAsTable("esi2hive.odl_osdata_cpu_index_tmp");
val loaddata = sqlContext.sql("INSERT INTO TABLE esi2hive.odl_osdata_cpu_index PARTITION (appName) (select insertTimestamp,eventTimestamp, cpuName, serverName , pctIOWait, pctIdle, pctSteal, pctSystem, pctUser, unique_id,appName  from esi2hive.odl_osdata_cpu_index_tmp where unique_id NOT IN (select distinct(unique_id) from esi2hive.odl_osdata_cpu_index))")

val cleandata=sqlContext.sql("DROP TABLE IF EXISTS esi2hive.odl_osdata_cpu_index_tmp")
System.exit(0)
