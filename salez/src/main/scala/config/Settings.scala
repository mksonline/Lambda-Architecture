package config

import com.typesafe.config.ConfigFactory

object Settings {
    private val config = ConfigFactory.load()

    object WebLogGen {
        private val weblogGen = config.getConfig("settings")
        lazy val sparkWd: String = weblogGen.getString("spark_wd")
        lazy val HadoopHome: String = weblogGen.getString("hadoop")
        lazy val checkPointDir: String = weblogGen.getString("checkPoint")
    }
}
