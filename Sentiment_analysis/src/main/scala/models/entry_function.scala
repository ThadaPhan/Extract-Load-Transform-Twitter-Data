package models

import edu.stanford.nlp.util.CoreMap

import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._


class entry_function {

  def sentimentText = (text: String) => {
    var mainSentiment = 0
    var longest = 0;
    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")
    val props = new Properties();
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
    new StanfordCoreNLP(props).process(text).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.foreach((sentence: CoreMap) => {
      val sentiment = RNNCoreAnnotations.getPredictedClass(sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree]));
      val partText = sentence.toString();
      if (partText.length() > longest) {
        mainSentiment = sentiment;
        longest = partText.length();
      }
    })
    sentimentText(mainSentiment)
  }
  val sentimentText_udf = udf(sentimentText)

  def who = (text: String) => {
    val text_lower = text.toLowerCase()
    var ronaldo = 0
    var messi = 0
    //    #ronaldo, #messi
    if(text_lower.contains("ronaldo") | text_lower.contains("cr7")){
      ronaldo = 1
    }

    if(text_lower.contains("messi") | text_lower.contains("m10")){
      messi = 1
    }
    if(ronaldo == messi)
      if(ronaldo == 1)
        "both"
      else
        "another"
    else
      if(messi == 1)
        "messi"
      else
        "ronaldo"
  }
  val who_udf = udf(who)

  def source = (text: String) => {
    text.split(">")(1).split("<")(0)

  }
  val source_udf = udf(source)
}
