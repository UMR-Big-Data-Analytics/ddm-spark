package de.ddm

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
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
//    println("Files: " + inputs)
    val tableNames = inputs.map(s => s.split("/")(2).split("\\.")(0)).toList
    println("Tables: " + tableNames)

    // Find necessary offset
    var offset = 10;
    while (inputs.length - offset >= 0) {
      offset *= 10
    }

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    var allColumns = Map[Int, Dataset[org.apache.spark.sql.Row]]()
    var allColumnSizes = Map[Int, Long]()
    var allColumnTypes = Map[Int, Int]()
    var tableId = 0



    val onlyDigitsRegex = "^\\d+$".r
    val intPattern = "-?\\d+".r
    val floatPattern = "-?\\d+(\\.\\d+)?".r
    val datePattern = "\\d+-\\d+-\\d+".r
    val phonePattern = "\\d+-\\d+-\\d+-\\d+".r


//    def isInt(x: String) = x match {
//      case intPattern() => true
//      case _ => false
//    }
//
//    def isFloat(x: String) = x match {
//      case floatPattern() => true
//      case _ => false
//    }
//
//    def isDate(x: String) = x match {
//      case datePattern() => true
//      case _ => false
//    }
//
//    def isPhone(x: String) = x match {
//      case phonePattern() => true
//      case _ => false
//    }




    for (tablePath <- inputs) {
      val table = spark
        .read
        .option("inferSchema", "false")
        .option("header", "true")
        .option("quote", "\"")
        .option("delimiter", ";")
        .csv(tablePath)

      var columnNo = 0
      for (columnName <- table.columns) {
        val columnId = tableId + columnNo * offset
        print(columnId + " " + columnName + ", ")
        val column = table.select(table.col(columnName)).distinct().persist()
        allColumns += (columnId -> column)
        allColumnSizes += (columnId -> column.count())

        val row = column.first().get(0).asInstanceOf[String]
        row match {
          case intPattern() => allColumnTypes += (columnId -> 0)
          case floatPattern() => allColumnTypes += (columnId -> 1)
          case datePattern() => allColumnTypes += (columnId -> 2)
          case phonePattern() => allColumnTypes += (columnId -> 3)
          case _ => allColumnTypes += (columnId -> 4)
        }

        columnNo += 1
      }
      tableId += 1
      println()
    }
//    println("\nAll columns: " + allColumns)

    // Generate candidates
    val candidates = allColumns.keys
      .flatMap(x => allColumns.keys.map(y => (x, y)))
      .filter(c => c._1 != c._2)
      .toBuffer
      .sortWith((x, y) => { // Sort candidates according to size descending because buffer is shortened from the end!
        allColumnSizes(x._1) + allColumnSizes(x._2) > allColumnSizes(y._1) + allColumnSizes(y._2)
      })
    // Check ordering
//    candidates.foreach(c => print((allColumnSizes(c._1) + allColumnSizes(c._2)) + ", "))
//    println()

    val INDs = ListBuffer[(Int, Int)]()
    val noINDs = ListBuffer[(Int, Int)]()


    def moveCandidateAtUnknownIndex(columnId1: Int, columnId2: Int, isIND: Boolean): Boolean = {
      val index =  candidates.indexOf((columnId1, columnId2))
      moveCandidateAtIndex(index, isIND)
    }

    def moveCandidateAtIndex(index: Int, isIND: Boolean): Boolean = {

      if (index >= 0) {
        val candidate = candidates.remove(index)

        if (isIND) {
          print(" i" + index + ",") // IND
          print("\nNew IND found!")
          val newINDs = ListBuffer[(Int, Int)]()
          val newNoINDs = ListBuffer[(Int, Int)]()
          for (ind <- INDs) {
            if (ind._2 == candidate._1) {
              newINDs += ((ind._1, candidate._2))
            } else  if (ind._1 == candidate._2) {
              newINDs += ((candidate._1, ind._2))
            }
          }
          for (noInd <- noINDs) {
            if (noInd._1 == candidate._1) {
              newNoINDs += ((candidate._2, noInd._2))
            } else  if (noInd._2 == candidate._2) {
              newNoINDs += ((noInd._1, candidate._1))
            }
          }
          INDs += candidate
          for (newInd <- newINDs) {
            moveCandidateAtUnknownIndex(newInd._1, newInd._2, true)
          }
          for (newNoInd <- newNoINDs) {
            moveCandidateAtUnknownIndex(newNoInd._1, newNoInd._2, false)
          }

        } else {
          print(" o" + index + ",") // NO IND
          val newNoINDs = ListBuffer[(Int, Int)]()
          for (ind <- INDs) {
            if (candidate._1 == ind._1) {
              newNoINDs += ((ind._2, candidate._2))
            } else  if (candidate._2 == ind._2) {
              newNoINDs += ((candidate._1, ind._1))
            }
          }
          noINDs += candidate
          for (newNoInd <- newNoINDs) {
            moveCandidateAtUnknownIndex(newNoInd._1, newNoInd._2, false)
          }
        }
      } else {  // Already moved
        if (isIND) {
          print(" i,")
        } else {
          print(" o,")
        }
      }
      true
    }


    // https://stackoverflow.com/questions/47028442/add-column-from-one-dataframe-to-another-dataframe-in-scala
    // https://sparkbyexamples.com/spark/spark-select-columns-from-dataframe/
    // https://sparkbyexamples.com/spark/spark-dataframe-withcolumn/

    while (candidates.nonEmpty) {
      val candidate = candidates.last
      print("\nTake next index")
        if (allColumnTypes(candidate._1) == allColumnTypes(candidate._2)) {
          val col1 = allColumns(candidate._1)
          val col2 = allColumns(candidate._2)
          val size1 = allColumnSizes(candidate._1)
          val size2 = allColumnSizes(candidate._2)
          val joinSize = col1.join(col2, col1(col1.columns(0)) === col2(col2.columns(0)), "inner").count()
          if (size1 < size2) {
            moveCandidateAtIndex(candidates.size -1, joinSize == size1)
            moveCandidateAtUnknownIndex(candidate._2, candidate._1, false)
          } else if (size2 < size1) {
            moveCandidateAtIndex(candidates.size -1, false)
            moveCandidateAtUnknownIndex(candidate._2, candidate._1, joinSize == size2)
          } else {
            moveCandidateAtIndex(candidates.size -1, joinSize == size1)
            moveCandidateAtUnknownIndex(candidate._2, candidate._1, joinSize == size2)
          }
        } else {
          moveCandidateAtIndex(candidates.size -1, false)
        }

    }

    println("\nCandidates: " + candidates)
    println("INDs: " + INDs)
    println("noINDs: " + noINDs)

    val INDMap: mutable.Map[String, ListBuffer[String]] = mutable.Map.empty.withDefaultValue(ListBuffer())
//    var INDList = ListBuffer[String]()
    var counter = 1

    for (ind <- INDs) {
      val tableId1 = ind._1 % offset
//      val columnId1 = ind._1 / offset
      val tableId2 = ind._2 % offset
//      val columnId2 = ind._2 / offset
      val colName1 = allColumns(ind._1).columns(0)
      val colName2 = allColumns(ind._2).columns(0)
      INDMap.update(colName1, INDMap(colName1) :+ colName2)

//      val INDString = tableNames(tableId1) + " -> " + tableNames(tableId2) + ": [" + colName1 + "] C [" + colName2 + "]\n"
//      println(counter + " " + INDString)
//      INDList += INDString

//      println(counter + " " + ind + ": " + tableId1 + "." +  columnId1 + " = " + tableNames(tableId1) + "." + allColumns(ind._1).columns(0)
//        + "  contained in  "
//        + tableId2 + "." +  columnId2 + " = " + tableNames(tableId2) + "." + allColumns(ind._2).columns(0))

      counter += 1
    }
    for (elem <- INDMap) {
      INDMap.update(elem._1, INDMap(elem._1).sorted)
    }
    val sorted = INDMap.keySet.toList.sorted

//    println(INDMap)
    for (key <- sorted) {
      print(key + " < " + INDMap(key).head)
      var counter = 0
      for (referenced <- INDMap(key)) {
        if (counter > 0) {
          print(", " + referenced)
        }
        counter += 1
      }
      println()
    }


//    // Write to file
//    import java.io._
//    val printWriter = new PrintWriter(new File("result.txt"))
//    counter = 1
//    INDList = INDList.sorted
//    for (string <- INDList) {
////      print(counter + " " + string)
//      printWriter.write(string)
//      counter += 1
//    }
//    printWriter.close()



  }
}
