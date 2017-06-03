package utils

import java.lang.management.ManagementFactory

import config.Settings.WebLogGen
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

    val isIDE: Boolean = {
        //If executing inside IDE
        ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
    }

    def getSparkSession: SparkSession = {
        //get spark configuration
        val conf = new SparkConf()
            .setAppName("Simple-Lambda")
            .set("spark.cassandra.connection.host", "localhost")
            .set("spark.local.dir", WebLogGen.sparkWd)

        val checkpointDirectory = WebLogGen.checkPointDir

        //If executing inside IDE
        if (isIDE) {
            System.setProperty("hadoop.home.dir", WebLogGen.HadoopHome)
            conf.setMaster("local[*]")
        }

        //init spark session
        val spark = SparkSession.builder()
            .config(conf)
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.setCheckpointDir(checkpointDirectory)

        spark
    }

}
