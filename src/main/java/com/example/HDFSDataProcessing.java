package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class HDFSDataProcessing {
    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("HDFS Data Processing")
                .getOrCreate();

        // Define the complete HDFS file path
        String hdfsFilePath = "/user/airflow/downloaded_datasets/top-12-german-companies/Top_12_German_Companies NEW.csv";

        // Read the file from HDFS using Spark
        Dataset<Row> data = spark.read().option("header", "true").csv(hdfsFilePath);

        // Transform the data
        Dataset<Row> transformedData = data.withColumn("Revenue", functions.col("Revenue").cast("double").divide(100));

        // Count the number of rows in the transformed data
        long result = transformedData.count();

        // Print the result
        System.out.println("Number of rows in transformed data: " + result);

        // Show the first 10 rows of the original data
        System.out.println("Original data:");
        data.show(10);

        // Show the first 10 rows of the transformed data
        System.out.println("Transformed data:");
        transformedData.show(10);

        // Stop the Spark session
        spark.stop();
    }
}

