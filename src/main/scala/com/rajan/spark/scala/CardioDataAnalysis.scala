package com.rajan.spark.scala

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{SaveMode, SparkSession}

object CardioDataAnalysis {

  def main(args: Array[String]): Unit = {
    println("----- Staring - Cardio Data Analysis!------")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Cardio Data Reader")
      .getOrCreate()

    val inputFile = args(0)
    val outputDirs = args(1)

    val sc = spark.sparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outPutPath = new Path(outputDirs)

    if (fs.exists(outPutPath)) {
      println(s" **** Deleting old output (if any), $outputDirs:")
      fs.delete(outPutPath, true)
    }

    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .load(inputFile)

    // Dataframe with dropped id and updated column
    val DF = df.drop("id").withColumn("age", (col("age") / 365.25).cast(DecimalType(26, 1)))
      .withColumn("gender", when(col("gender").equalTo("2"), "0")
        .otherwise(col("gender"))
      )
    // val DF = df.drop("id").withColumn("age", (col("age") / 365.25).cast("Integer"))

    DF.show();

    val totalDataCount = DF.count();
    val distintDF = DF.distinct().count();
    val duplicateRecords = totalDataCount - distintDF;

    // Dropped duplicated records
    val cleanedDF = DF.dropDuplicates();

    val dFWithBMI = cleanedDF.withColumn("bmi", (col("weight") / (col("height") * col("height") / 10000)).cast(DecimalType(26, 1)));
    dFWithBMI.show();
    dFWithBMI.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/dataWithBMI")

    //------------
    // Group users by age group who has suffered from cardio disease
    val groupedByAgeGroup = DF.groupBy("age").sum("cardio").withColumnRenamed("age", "years").withColumnRenamed("sum(cardio)", "count")
    //    groupedByAgeGroup.show();

    groupedByAgeGroup.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/age_grouped_count")
    //------------

    //------------
    val df_categorical = cleanedDF.groupBy("cholesterol", "gluc", "smoke", "alco", "active").sum("cardio")
    //    df_categorical.show();
    //------------

    //------------
    val df_categorical_cardio1 = cleanedDF.filter(df("cardio") === 1).groupBy("cholesterol", "gluc", "smoke", "alco", "active").count()
    df_categorical_cardio1.show();

    val df_categorical_cardio0 = cleanedDF.filter(df("cardio") === 0).groupBy("cholesterol", "gluc", "smoke", "alco", "active").count()
    df_categorical_cardio0.show();

    df_categorical.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/categorical1")
    df_categorical_cardio1.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/categorical_cardio1")
    df_categorical_cardio0.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/categorical_cardio0")
    //------------

    //-------------Drop weight and height
    val reducedWeightAndHeight = dFWithBMI.drop("weight", "height")
    reducedWeightAndHeight.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(outputDirs + "/reducedWeightAndHeight")

    //    val reducedWeightAndHeight = dFWithBMI.filter(dFWithBMI("bmi") > 10 && dFWithBMI("bmi") < 100).count()
    //    printf("filteredFromBmi: %s", filteredFromBmi)

    //-------------

    printf("Total records: %s", totalDataCount)
    println()
    printf("Total duplicates: %s", duplicateRecords)
    println()
    printf("Total cleaned records: %s", cleanedDF.count())
    println()

    //    val totalDataCount = df.count();

    //    val grouped = df.groupBy("gender").sum("cardio").show()
    //    val groupedNew = df.groupBy("gender", "weight").sum("cardio").show()

    // Percentage of users with following conditions
    //    val filteredData = df.filter(
    //      df("smoke") === 1 &&
    //        df("gender") === 1 &&
    //        df("cardio") === 1
    //    ).count()

    //    val totalPercentage = (filteredData / totalDataCount);
    //
    //    println()
    //    printf("Total records: %s", totalDataCount)
    //    println()
    //    printf("Total filtered records: %s", filteredData)
    //    println()
    //    printf("Total percentage: %s", totalPercentage)
    //    println()
    //    print(grouped)

    //  df.show()

  }
}
