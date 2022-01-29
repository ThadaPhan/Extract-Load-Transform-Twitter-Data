
import com.github.nscala_time.time.Imports.{richDateTime, richInt}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import models.entry_function
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object TwitterAnalysis {
  val function = new entry_function();
  val sentimentText_udf = function.sentimentText_udf
  val who_udf = function.who_udf
  val source_udf = function.source_udf


  def main(args: Array[String]): Unit = {

    val schema = StructType(Array(
      StructField("tweet_id",StringType,true),
      StructField("fav_count",StringType,true),
      StructField("create_at",StringType,true),
      StructField("text",StringType,true),
      StructField("source",StringType,true),
      StructField("user_id",StringType,true),
      StructField("link",StringType,true),
    ))

    val sparkSession = SparkSession.builder()
      .appName("Twitter Analysis")
      //.master("spark://master:7077")
      .enableHiveSupport()
      .getOrCreate()

    val sparkSQL = sparkSession.sqlContext

    val date_time_format = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:00")


    val now = DateTime.now + 7.hours
    val start = date_time_format.print(now - 30.minutes)
    val end = date_time_format.print(now + 1.minutes)

    println("now: ", now)
    println("start: ", start)
    println("end: ", end)

    val sql ="select tweet_id, fav_count, create_at, text, source, user_id, link " +
             "from (select tweet_id, max(fav_count) as fav_count " +
             "from twitter.tweet_favorite_count " +
             "where now >= " + "'" + start + "'" + "and now < " + "'" + end + "' " +
             "group by tweet_id, fav_count) as fav inner join twitter.tweet on tweet.id = fav.tweet_id"

    val tweet_dataframe = sparkSQL.read.format("jdbc")
      .option("url", "jdbc:mysql://172.16.0.5/twitter")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sql ) t")
      .option("user", "root")
      .option("password", "123")
      .schema(schema)
      .load()

    val df_sentiment_score = tweet_dataframe.withColumn("score", sentimentText_udf(col("text")))

    val df_who = df_sentiment_score.withColumn("who", who_udf(col("text")))

    val df_who_filter = df_who.filter("who like 'messi' or who like 'ronaldo'")

    val df_source = df_who_filter.withColumn("media_source", source_udf(col("source"))).repartition(1)

    df_source.withColumn("fav_count",col("fav_count").cast("integer"))

    df_source.show()

    df_source.write.mode("append").saveAsTable("twitter.minutes")

    sparkSession.stop()
  }

}
