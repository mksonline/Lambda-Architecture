package cassandra

import com.datastax.driver.core.{ResultSet, Session}
import com.datastax.spark.connector.cql.CassandraConnector
import model.ProvinceSale
import org.apache.spark.sql.SparkSession

object Satement {
    def getConnector(spark: SparkSession): CassandraConnector ={
        val connector = CassandraConnector.apply(spark.sparkContext.getConf)
        connector
    }

    private def cql_update(prod: String, state: String, total: Long): String =
        s"""insert into demo1.sales (prod,state,total) values ('$prod', '$state', $total)""".stripMargin

    def updateProvinceSale(connector: CassandraConnector, value: ProvinceSale): ResultSet = {
        connector.withSessionDo { session =>
            session.execute(cql_update(value.prod, value.state, value.total))
        }
    }

    def createKeySpaceAndTable(session: Session, dropTable: Boolean = false): ResultSet = {
        session.execute(
            """CREATE KEYSPACE  if not exists  demo1 WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 };""")
        if (dropTable)
            session.execute("""drop table if exists my_keyspace.test_table""")

        session.execute(
            """create table if not exists demo1.sales (prod text, state text, total int, PRIMARY KEY (prod, state));""")
    }
}
