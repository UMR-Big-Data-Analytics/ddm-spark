package de.ddm

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.{Encoder, SparkSession}

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

// A non-case class; requires an encoder to work as Dataset type
class Pet(var name:String, var age:Int) {
  override def toString = s"Pet(name=$name, age=$age)"
}

object Tutorial {

  def execute(spark: SparkSession): Unit = {

    //------------------------------------------------------------------------------------------------------------------
    // Lamda basics (for Scala)
    //------------------------------------------------------------------------------------------------------------------

    // Spark uses user defined functions to transform data, lets first look at how functions are defined in scala:
    val smallListOfNumbers = List(1, 2, 3, 4, 5)

    // A Scala map function from int to double
    def squareAndAdd(i: Int): Double = {
      i * 2 + 0.5
    }
    // A Scala map function defined in-line (without curly brackets)
    def squareAndAdd2(i: Int): Double = i * 2 + 0.5
    // A Scala map function inferring the return type
    def squareAndAdd3(i: Int) = i * 2 + 0.5
    // An anonymous Scala map function assigned to a variable
    val squareAndAddFunction = (i: Int) => i * 2 + 0.5

    // Different variants to apply the same function
    println(smallListOfNumbers.map(squareAndAdd))
    println(smallListOfNumbers.map(squareAndAdd2))
    println(smallListOfNumbers.map(squareAndAdd3))
    println(smallListOfNumbers.map(squareAndAddFunction))
    println(smallListOfNumbers.map(i => i * 2 + 0.5)) // anonymous function; compiler can infers types
    println(smallListOfNumbers.map(_ * 2 + 0.5)) // syntactic sugar: '_' maps to first (second, third, ...) parameter

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Important imports
    //------------------------------------------------------------------------------------------------------------------

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // Create a Dataset programmatically
    val numbers = spark.createDataset((0 until 100).toList)

    // Read a Dataset from a file
    val employees = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/employees.csv") // also text, json, jdbc, parquet
      .as[(String, Int, Double, String)]

    employees.show()

    // Read a Dataset from a database: (requires a database being setup as well as driver class in maven)
    //    val top_templates = spark.sqlContext.read.format("jdbc")
    //      .option("url", "jdbc:postgresql://localhost/changedb")
    //      .option("driver", "org.postgresql.Driver")
    //      .option("useUnicode", "true")
    //      .option("useSSL", "false")
    //      .option("user", "dummy")
    //      .option("password", "dummy")
    //      .option("dbtable","templates_infoboxes")
    //      .load()

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Basic transformations
    //------------------------------------------------------------------------------------------------------------------

    // Basic transformations on datasets return new datasets
    val mapped = numbers.map(i => "This is a number: " + i)
    val filtered = mapped.filter(s => s.contains("1"))
    val sorted = filtered.sort()
    List(numbers, mapped, filtered, sorted).foreach(dataset => println(dataset.getClass))
    sorted.show()

    println("---------------------------------------------------------------------------------------------------------")

    // Basic terminal operations
    val collected = filtered.collect() // collects the entire dataset to the driver process
    val reduced = filtered.reduce((s1, s2) => s1 + "," + s2) // reduces all values successively to one
    filtered.foreach(s => println(s)) // performs an action for each element (take care where the action is evaluated!)
    List(collected, reduced).foreach(result => println(result.getClass))

    println("---------------------------------------------------------------------------------------------------------")

    // DataFrame and Dataset
    val untypedDF = numbers.toDF() // DS to DF
    val stringTypedDS = untypedDF.map(r => r.get(0).toString) // DF to DS via map
    val integerTypedDS = untypedDF.as[Int] // DF to DS via as() function that cast columns to concrete types
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head.getClass))
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head))

    println("---------------------------------------------------------------------------------------------------------")

    // Mapping to tuples
    numbers
      .map(i => (i, "nonce", 3.1415, true))
      .take(10)
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    // SQL on DataFrames
    employees.createOrReplaceTempView("employee") // make this dataframe visible as a table
    val sqlResult = spark.sql("SELECT * FROM employee WHERE Age > 95") // perform an sql query on the table

    import org.apache.spark.sql.functions._

    sqlResult // DF
      .as[(String, Int, Double, String)] // DS
      .sort(desc("Salary")) // desc() is a standard function from the spark.sql.functions package
      .head(10)
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    // Grouping and aggregation for Datasets
    val topEarners = employees
      .groupByKey { case (name, age, salary, company) => company }
      .mapGroups { case (key, iterator) =>
        val topEarner = iterator.toList.maxBy(t => t._3) // could be problematic: Why?
        (key, topEarner._1, topEarner._3)
      }
      .sort(desc("_3"))
    topEarners.collect().foreach(t => println(t._1 + "'s top earner is " + t._2 + " with salary " + t._3))

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Analyzing Datasets and DataFrames
    //------------------------------------------------------------------------------------------------------------------

    employees.printSchema() // print schema of dataset/dataframe
    topEarners.explain() // print Spark's physical query plan for this dataset/dataframe
    topEarners.show() // print the content of this dataset/dataframe

    //------------------------------------------------------------------------------------------------------------------
    // Dates and Null Values
    //------------------------------------------------------------------------------------------------------------------

    println("---------------------------------------------------------------------------------------------------------")

    import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, col, date_add}

    // Create a data frame with 5 records holding a date, a timestamp, and a null column
    val dateDF = spark.range(5)
      .withColumn("date_today", current_date())
      .withColumn("stamp_now", current_timestamp())
      .withColumn("nulls", lit(null).cast("string"))
    dateDF.show()

    println("---------------------------------------------------------------------------------------------------------")

    // Fill nulls and move date to next week
    val filledNulls = dateDF
      .na.fill("no_null_value", Seq("date_today", "stamp_now", "nulls")) // fill nulls
      .select(
        col("id"),
        date_add(col("date_today"), 7).as("date_next_week"), // date next week
        col("nulls").as("no_nulls"),
        col("nulls").isNull.as("is_null")) // is-null-check
    filledNulls.show()

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Custom types
    //------------------------------------------------------------------------------------------------------------------

    // (see definition above) A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
    //   case class Person(name:String, surname:String, age:Int)

    val persons = spark.createDataset(List(
      Person("Barack", "Obama", 40),
      Person("George", "R.R. Martin", 65),
      Person("Elon", "Musk", 34)))

    persons
      .map(_.name + " says hello")
      .collect()
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    // (see definition above) A non-case class; requires an encoder to work as Dataset type
    //   class Pet(var name:String, var age:Int) {
    //     override def toString = s"Pet(name=$name, age=$age)"
    //   }

    implicit def PetEncoder: Encoder[Pet] = org.apache.spark.sql.Encoders.kryo[Pet]

    val pets = spark.createDataset(List(
      new Pet("Garfield", 5),
      new Pet("Paddington", 2)))

    pets
      .map(_ + " is cute") // our Pet encoder gets passed to method implicitly
      .collect()
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Shared Variables across Executors
    //------------------------------------------------------------------------------------------------------------------

    // The problem: shipping large variables (multiple times) to every executor is expensive
    val names = List("Berget", "Bianka", "Cally")
    val filtered1 = employees.filter(e => names.contains(e._1)) // a copy of names is shipped to every executor
    val filtered2 = employees.filter(e => !names.contains(e._1)) // a copy of names is shipped to every executor again!
    val filtered3 = employees.filter(e => names(1).equals(e._1)) // a copy of names is shipped to every executor again!
    List(filtered1, filtered2, filtered3).foreach(_.show(1))

    println("---------------------------------------------------------------------------------------------------------")

    // Solution: broadcast variable
    val bcNames = spark.sparkContext.broadcast(names)
    val bcFiltered1 = employees.filter(e => bcNames.value.contains(e._1)) // a copy of names is shipped to each executor node
    val bcFiltered2 = employees.filter(e => !bcNames.value.contains(e._1)) // a copy of names is already present on the node
    val bcFiltered3 = employees.filter(e => bcNames.value(1).equals(e._1)) // a copy of names is already present on the node
    List(bcFiltered1, bcFiltered2, bcFiltered3).foreach(_.show(1))
    bcNames.destroy() // finally, destroy the broadcast variable to free its memory on every node

    println("---------------------------------------------------------------------------------------------------------")

    // Accumulators:
    // - shared variables that distributed executors can add to
    // - enable side-effects in pipeline computations (e.g. for debugging)
    // - only usable in actions, not in transformations (transformations should not have side effects as they may be re-run!)
    // - should be used with care: http://imranrashid.com/posts/Spark-Accumulators/
    val appleAccumulator = spark.sparkContext.longAccumulator("Apple Accumulator")
    val microsoftAccumulator = spark.sparkContext.longAccumulator("Microsoft Accumulator")
    employees.foreach(
      e => if (e._4 == "Apple") appleAccumulator.add(1)
      else if (e._4 == "Microsoft") microsoftAccumulator.add(1)
      /* ... */) // accumulators are useful only if the action also does something else is; otherwise use filter and count!
    println("There are " + appleAccumulator.value + " employees at Apple")
    println("There are " + microsoftAccumulator.value + " employees at Microsoft")

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Machine Learning
    // https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#decision-tree-classifier
    //------------------------------------------------------------------------------------------------------------------

    val data = spark
      .read
      .format("libsvm")
      .load("data/sample_libsvm_data.txt")

    data.printSchema() // Spark reads the libsvm data into a dataframe with two columns: label and features
    data.show(10)

    println("---------------------------------------------------------------------------------------------------------")

    // Concepts in Spark's machine learning module:
    // - Transformer: An algorithm (function) that transforms one DataFrame into another DataFrame.
    //                I.e. an ML model that transforms a DataFrame of features into a DataFrame of predictions.
    // - Estimator:   An algorithm (function) that trains a Transformer on a DataFrame.
    //                I.e. a learning algorithm (e.g. DecisionTree) that trains on a DataFrame and produces a model.
    // - Pipeline:    A directed acyclic graph (DAG) chaining multiple Transformers and Estimators together.
    //                I.e. a ML workflow specification.

    // Automatically identify categorical features, and index them.
    // The Vectors contain only numerical values, so we need to flag which values are categorical
    // A VectorIndexer is a transformer that, in this case, adds a column.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(data) // fit the indexer to the data (= "parameterize" and not "train")

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Create a Decision Tree Classifier
    val decisionTree = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, decisionTree))

    // Run the entire pipeline:
    // 1. The featureIndexer adds the column "indexedFeatures"
    // 2. The decisionTree trains the decision tree model
    val model = pipeline.fit(trainingData) // produces a model, which is a new transformer

    // Print the learned decision tree model
    val treeModel = model
      .stages(1)
      .asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

    println("---------------------------------------------------------------------------------------------------------")

    // Make predictions in an additional column in the output dataframe with default name "prediction"
    val predictions = model.transform(testData)

    // Select example rows to display
    predictions
      .select("prediction", "label", "features", "indexedFeatures")
      .show(10)

    // Evaluate the error for the predictions on the test dataset using its predicted and true labels
    val evaluator = new MulticlassClassificationEvaluator() // a convenience class for calculating a model's accuracy
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test error = " + (1.0 - accuracy))

    println("---------------------------------------------------------------------------------------------------------")

    // Playground for exam tasks:
/*
    val students = spark
      .read
      .option("quote", "\"")
      .option("delimiter", ",")
      .csv(s"data/students.csv")
      .toDF("ID", "Name", "Semester", "Supervisor")
      .as[(String, String, String, String)]
    val enrollments = spark
      .read
      .option("quote", "\"")
      .option("delimiter", ",")
      .csv(s"data/enrollments.csv")
      .toDF("StudentID", "CourseID", "Credits")
      .as[(String, String, String)]
    val courses = spark
      .read
      .option("quote", "\"")
      .option("delimiter", ",")
      .csv(s"data/courses.csv")
      .toDF("ID", "Title", "Teacher", "Topic")
      .as[(String, String, String, String)]

    val result = students
      .filter(s => Integer.valueOf(s._3) > 3)
      .map(s => (hash(s._2), s._4))
      .toDF("HashedName", "Supervisor")
      .collect()
*/
  }
}
