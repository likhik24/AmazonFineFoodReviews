/* *****************************************************
* This program requires two arguments:
*  Argument 1: Location of preprocessed amazon food review file: foodProcessed.csv
*  Argument 2: Location of output file classifying spammers: amazon/s3/output/
*  Dataset for food review was downloaded from:
*  http://snap.stanford.edu/data/web-FineFoods.html
* *****************************************************/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._



object AmazonFineFoodsReview {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    if (args.length != 2) {
      println("two arguments needed")
    }
    Logger.getLogger("WordCount").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")

    val sc = spark.sparkContext

    import spark.implicits._
    val AmazonFoodReview = spark.read.option("header", "true").csv(args(0)).select("UserId", "ProductId", "Score", "Summary", "Time")

    val AmazonFoodReviewCasted = AmazonFoodReview.withColumn("Scores", 'Score.cast("Int")).select('UserId, 'ProductId, 'Scores as 'Score, 'Summary, 'Time)
    //AmazonFoodReviewCasted.show()

    //Stanford nlp gives sentiment from 0-4(0 -> strong negative statement,4 -> strong positive statement)
    //Changing the AmazonFoodReviewCasted dataframe Score(Rating) values from 5 -> 4 , 4 -> 3, 3 -> 2, 2 -> 1, 1 -> 0
    //to make both Score(Rating) and Sentiment Score on same scale i.e (0-4)
    val AmazonFoodReviewDF = AmazonFoodReviewCasted.withColumn("Score", col("Score") - 1)

    //CoreNLP sentiment analysis on reviews
    val AmazonFoodReviewWithSentiment = AmazonFoodReviewDF
      .select((('Summary).as('doc)), 'UserId, 'ProductId, 'Score, 'Time)
      .select((explode(ssplit('doc)).as('sen)), 'UserId, 'ProductId, 'Score, 'Time)
      .select('UserId, 'ProductId, 'sen, sentiment('sen).as('Sentiment), 'Score, 'Time).withColumn("sentiment1", 'Sentiment.cast("Int")).select('UserId, 'ProductId, 'sen, 'sentiment1 as 'Sentiment, 'Score, 'Time)

    AmazonFoodReviewWithSentiment.show(truncate = false)

    val AmazonFoodReviewWithSentimentCols = AmazonFoodReviewWithSentiment.select("ProductId", "UserId", "Score", "Sentiment")
    val SpamReviewersForSure = AmazonFoodReviewWithSentimentCols.filter((AmazonFoodReviewWithSentimentCols.col("Sentiment") <= (AmazonFoodReviewWithSentimentCols.col("score") - 2)) || (AmazonFoodReviewWithSentimentCols.col("score") <= (AmazonFoodReviewWithSentimentCols.col("Sentiment") - 2)))
    val AmazonFoodReviewWithSentimentRest = AmazonFoodReviewWithSentimentCols.except(SpamReviewersForSure)
    val AmazonFoodReviewAvgStd = AmazonFoodReviewWithSentimentRest.groupBy($"ProductId").agg(mean(AmazonFoodReviewWithSentimentRest("Score")) as "Avg_Score", stddev_pop(AmazonFoodReviewWithSentimentRest("Score")) as "Std_Dev_Score")
    val df1 = AmazonFoodReviewWithSentimentRest.as("abc")
    val df2 = AmazonFoodReviewAvgStd.as("def")
    val AmazonFoodReviewSentiAvgStd = df1.join(df2, col("abc.ProductId") === col("def.ProductId"))

    //classifying spam reviews
    val negativeDF1 = AmazonFoodReviewSentiAvgStd.filter((AmazonFoodReviewSentiAvgStd.col("Score") < (AmazonFoodReviewSentiAvgStd.col("Avg_Score") - AmazonFoodReviewSentiAvgStd.col("Std_Dev_Score"))) || (AmazonFoodReviewSentiAvgStd.col("Score") > (AmazonFoodReviewSentiAvgStd.col("Avg_Score") + AmazonFoodReviewSentiAvgStd.col("Std_Dev_Score"))))
    val finalNegativeDF = negativeDF1.select("abc.ProductId", "abc.UserId", "abc.Score", "abc.Sentiment").union(SpamReviewersForSure)
    //All spam reviewers classified
    val totalNegative = finalNegativeDF.groupBy("UserId").count().select($"UserId", $"count".alias("negativeCount"))
    val AllUser = AmazonFoodReviewWithSentimentCols.groupBy("UserId").count().filter($"count" >= 0).select($"UserId", $"count".alias("totalCount"))
    val d1 = totalNegative.as("negativetotal")
    val d2 = AllUser.as("total")
    val joinedDF2 = d1.join(d2, col("negativetotal.UserId") === col("total.UserId"))
    val negPosUsersCount = joinedDF2.filter($"negativetotal.negativeCount" / $"total.totalCount" > 0.5).select($"negativetotal.userId", $"negativetotal.negativeCount", ($"total.totalCount" - $"negativetotal.negativeCount") as "positiveCount")
    val negUsersDescending = negPosUsersCount.sort(desc("negativeCount")).withColumn("negativeCount", when(($"negativeCount" > 0),1).otherwise(0)).drop("positiveCount")
    val allUsers = joinedDF2.select($"negativetotal.userId", $"negativetotal.negativeCount", ($"total.totalCount" - $"negativetotal.negativeCount") as "positiveCount", $"total.totalCount" )

    //Joining spammers with all users based on userID
    val total_users = AmazonFoodReviewWithSentiment.join(negUsersDescending, Seq("UserId"), "left_outer")
    //Adding spam = 0 for non spammers
    val final_df= total_users.na.fill(0, Seq("negativeCount"))
    val newNames = Seq("UserId", "ProductId", "Summary", "Sentiment", "Score","Timestamp", "spam")
    val AmazonFoodReviewSpammerClassified = final_df.toDF(newNames: _*)

    //Writing final classified Amazon food reviews
    AmazonFoodReviewSpammerClassified.rdd.saveAsTextFile(args(1))



  }
}


