package cs523.project_spark.hive;

import java.io.File;

import org.apache.spark.sql.SparkSession;

public class SparkHive {

	// warehouseLocation points to the default location for managed databases
	// and tables
	String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
	SparkSession spark = SparkSession.builder()
			.appName("Java Spark Hive")
			.config("spark.master", "local")
			.config("hive.metastore.uris",
			"thrift://127.0.0.1:9083")
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.enableHiveSupport()
			.getOrCreate();

	public void createTable() {
		spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS twitter (key STRING, value STRING) USING hive");
	}

	public void loadData(String path) {
		spark.sql("LOAD DATA INPATH '" + path + "' INTO TABLE twitter");
	}
	
	
	public static void main(String[] args){
		
		
		
	}
}
