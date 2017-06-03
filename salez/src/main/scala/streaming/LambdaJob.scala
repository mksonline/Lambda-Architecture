package streaming

import cassandra._
import model.{ProductSale, ProvinceSale}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SparkUtils.getSparkSession


object LambdaJob {
    def main(args: Array[String]): Unit = {

        // setup spark context
        val spark: SparkSession = getSparkSession
        import spark.implicits._

        // load stream from kafka
        val input: DataFrame = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "sales")
            .load()

        // select the values
        val df = input.selectExpr("CAST(value AS STRING)").as[String]

        // transform the raw input to a Dataset of model.Clients
        val ds = df.map(r => r.split(",")).map(c => ProductSale(c(0).toInt, c(1), c(2), c(3).toInt, c(4), c(5), c(6),c(7), c(8), c(9)))

        // do continuous aggregation
        val aggDF = ds.groupBy("prod", "state").count()

        // transform aggregated dataset to a Dataset of model.ProvinceSale
        val provincialSale = aggDF.map(r => ProvinceSale(r.getString(0), r.getString(1), r.getLong(2)))

        ///// CASSANDRA ////
        val connector = Satement.getConnector(spark)

        // This Foreach sink writer writes the output to cassandra.
        import org.apache.spark.sql.ForeachWriter
        val writer = new ForeachWriter[ProvinceSale] {
            override def open(partitionId: Long, version: Long) = true
            override def process(value: ProvinceSale): Unit = {
                Satement.updateProvinceSale(connector, value)
            }
            override def close(errorOrNull: Throwable): Unit = {}
        }
        ///// CASSANDRA END ////

        val query: StreamingQuery = provincialSale.writeStream
            .queryName("ProvincialSales")
            .outputMode("complete")
            .foreach(writer)
            .start()

        query.awaitTermination()
    }
}
