package de.ddm

import org.apache.spark.sql.SparkSession

object LongestCommonSubstring {

  def discoverLCSs(input: String, spark: SparkSession): Unit = {

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    // A simple function that finds the LCS between two given strings
    def longestCommonSubstring(str1: String, str2: String): String = {
      if (str1.isEmpty || str2.isEmpty)
        return ""

      var currentRow = new Array[Int](str1.length)
      var lastRow = if (str2.length > 1) new Array[Int](str1.length) else null
      var longestSubstringLength = 0
      var longestSubstringStart = 0

      var str2Index = 0
      while (str2Index < str2.length) {
        val str2Char = str2.charAt(str2Index)
        var str1Index = 0
        while (str1Index < str1.length) {
          var newLength = 0
          if (str1.charAt(str1Index) == str2Char) {
            newLength = if (str1Index == 0 || str2Index == 0) 1
            else lastRow(str1Index - 1) + 1
            if (newLength > longestSubstringLength) {
              longestSubstringLength = newLength
              longestSubstringStart = str1Index - (newLength - 1)
            }
          }
          else newLength = 0
          currentRow(str1Index) = newLength

          str1Index += 1
          str1Index - 1
        }
        val temp = currentRow
        currentRow = lastRow
        lastRow = temp

        str2Index += 1
        str2Index - 1
      }
      return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength)
    }

    // Read the input file
    val students = spark
      .read
      .option("inferSchema", "false")
      .option("header", "false")
      .option("quote", "\"")
      .option("delimiter", ",")
      .csv(s"data/$input.csv")
      .toDF("ID", "Name", "Password", "Gene")
      .as[(String, String, String, String)]

    val result = students
      .repartition(32)
      //.joinWith(students, students1.col("ID") =!= students2.col("ID"))
      .crossJoin(students).filter(r => !r.getString(0).equals(r.getString(4)))
      .as[(String, String, String, String, String, String, String, String)]
      .map(t => (t._1, longestCommonSubstring(t._4, t._8)))
      .groupByKey(t => t._1)
      .mapGroups{ (key, iterator) => (key, iterator
        .map(t => t._2)
        .reduce((a,b) => { if (a.length > b.length) a else b })) }
      .toDF("ID", "Substring")
      .show(200)
  }
}
