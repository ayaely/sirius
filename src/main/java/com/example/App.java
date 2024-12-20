package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class App {
    public static void main(String[] args) {
        // Initialize SparkSession with Hive support
        SparkSession spark = SparkSession.builder()
            .appName("TransformedSparkJob")
            .master("yarn")  // Use YARN as the master in a cluster
            .config("spark.sql.catalogImplementation", "hive")  // Enable Hive support
            .enableHiveSupport()  // Enable Hive support in SparkSession
            .getOrCreate();
        
        // HDFS input path
        String hdfsInputPath = "/user/airflow/downloaded_datasets/top-12-german-companies/Top_12_German_Companies NEW.csv";

        // Read the CSV file into a DataFrame

	  Dataset<Row> data = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(hdfsInputPath);

        // Select only the essential columns for the demo
        Dataset<Row> selectedData = data.select("Company", "Period", "Revenue");

        // Perform the transformation: Divide "Revenue" by 100
        Dataset<Row> transformedData = selectedData
                .withColumn("Revenue", functions.col("Revenue").cast("double").divide(100));

        // Save the transformed data to Hive
        transformedData.write()
                .mode("overwrite")  // Overwrite existing data if needed
                .saveAsTable("top_12_german_companies_transformed");

        spark.stop();
    }
}

