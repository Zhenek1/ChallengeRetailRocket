import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import scala.util.Random
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Administrator on 12/2/2015.
 */
object MLMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MLMain")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:\\Downloads\\yoochoose-dataFull\\session_features.dat")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()
    val splits = parsedData.randomSplit(Array(0.8, 0.2))
    val (trainingData, testData) = (splits(0), splits(1))
    var numClasses = 2
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32
    val treeStrategy = Strategy.defaultStrategy("Classification")
    val numTrees = 5
    val model = RandomForest.trainClassifier(trainingData, treeStrategy,
      numTrees, featureSubsetStrategy, Random.nextInt())
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val correct_preds = labelAndPreds.filter(x => x._1 == x._2)
    val wrong_preds = labelAndPreds.filter(x => x._1 != x._2)
    val truePositives = correct_preds.filter(x => x._1 != 0).count.toDouble
    println("Accuracy = " + (correct_preds.count.toDouble / testData.count))
    println("Precision = " + (truePositives / (truePositives + wrong_preds.filter(x => x._2 == 1).count.toDouble)))
    println("Recall = " + (truePositives /  (truePositives + wrong_preds.filter(x => x._1 == 1).count.toDouble)))

   /* val testErr = testData.map { point =>
      val prediction = model.predict(point.features)
      if (point.label == prediction) 1.0 else 0.0
    }.mean()
    println("Test Error = " + testErr)*/
    sc.stop()
  }
}
