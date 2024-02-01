package de.ddm

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Sindy {

  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark
      .read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .csv(input)
  }


  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    println("Files: " + inputs)

    val tableNames = inputs.map(s => s.split("/")(2).split("\\.")(0)).toList
    println(tableNames)

    // Find necessary offset
    var offset = 10;
    while (inputs.length - offset >= 0) {
      offset *= 10
    }


    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._
    //    var allColumns = spark.emptyDataFrame
    //    var tables = Map[Int, DataFrame]()
    var allColumns = Map[Int, Dataset[org.apache.spark.sql.Row]]()
    var allColumnSizes = Map[Int, Long]()
    var tableId = 0

    for (tablePath <- inputs) {
      val table = spark
        .read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(tablePath)
      //        .toDF("ID", "Name", "Password", "Gene")
      //        .as[(String, String, String, String)]
      //      tables.show()
      //      tables += (tableId1 -> table)
      var columnNo = 0

      for (columnName <- table.columns) {
        print("\n" + tableId + " " + columnName)
        val columnId = tableId + columnNo * offset
        val column = table.select(table.col(columnName)).distinct()
        allColumnSizes += (columnId -> column.count())
        allColumns += (columnId -> column)
        columnNo += 1
      }

      tableId += 1
    }
    println()
    println(allColumns)


    // Generate candidates
    //    var candidates = List(Tuple2[Int, Int])
    var candidates = allColumns.keys
      .flatMap(x => allColumns.keys.map(y => (x, y)))
      .filter(c => c._1 != c._2)
      .toBuffer

    var INDs = ListBuffer[(Int, Int)]()
    var noINDs = ListBuffer[(Int, Int)]()


    def moveUnknownIndex(columnId1: Int, columnId2: Int, isIND: Boolean): Boolean = {
      val index =  candidates.indexOf((columnId1, columnId2))
      moveIndex(index, isIND)
    }

    def moveIndex(index: Int, isIND: Boolean): Boolean = {
      print(", removing " + index)
      if (index >= 0) {
        if (isIND) {
          INDs += candidates.remove(index)
          println("IND found!")
          //          // Todo: pruning!
          //          for (index <- candidates.size - 1 until 0)
          //          for (ind <- INDs) {
          //            for (c <- candidates) {
          //              if (ind._1 == c._2) {
          //                move(c._1, ind._2, true)
          //              }
          //            }
          //          }
        } else {
          noINDs += candidates.remove(index)
        }
      }
      true
    }


    // https://stackoverflow.com/questions/47028442/add-column-from-one-dataframe-to-another-dataframe-in-scala
    // https://sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
    // https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/

//    for (i <- candidates.size - 1 until 0) { // Todo: candidates.size - 1 until 0
//      val candidate = candidates(i)
//      println(", " + i + candidate)
    while (candidates.nonEmpty) {
      val candidate = candidates.last
      println(", " + candidate)
//      if (!noINDs.contains(candidate) && !INDs.contains(candidate)) {
        val col1 = allColumns(candidate._1)
        val col2 = allColumns(candidate._2)
        val size1 = allColumnSizes(candidate._1)
        val size2 = allColumnSizes(candidate._2)
        val joinSize = col1.join(col2, col1(col1.columns(0)) === col2(col2.columns(0)), "inner").count()
        if (size1 < size2) {
          moveIndex(candidates.size -1, joinSize == size1)
          moveUnknownIndex(candidate._2, candidate._1, false)
        } else if (size2 < size1) {
          moveIndex(candidates.size -1, false)
          moveUnknownIndex(candidate._2, candidate._1, joinSize == size2)
        } else {
          moveIndex(candidates.size -1, joinSize == size1)
          moveUnknownIndex(candidate._2, candidate._1, joinSize == size2)
//          if (joinSize == size1) {
//            println(col1.columns(0) + " in " + col2.columns(0) + " or " + col2.columns(0) + " in " + col1.columns(0))
//          }
//        }
      }
    }

    println("Candidates: " + candidates)
    println("INDs: " + INDs)
    println("noINDs: " + noINDs)

    for (ind <- INDs) {
      val tableId1 = ind._1 % offset
      val columnId1 = ind._1 / offset
      val tableId2 = ind._2 % offset
      val columnId2 = ind._2 / offset
      println(ind + ": " + tableId1 + "." +  columnId1 + " = " + tableNames(tableId1) + "." + allColumns(ind._1).columns(0)
        + "  contained in  "
        + tableId2 + "." +  columnId2 + " = " + tableNames(tableId2) + "." + allColumns(ind._2).columns(0))
      // Todo write to file
    }




  }
}
