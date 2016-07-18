package impressions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class GenerateReports {

	public static void generateCountOfUsers(DataFrame df1, DataFrame df2) {
		Column joinExpr = (df1.col("user_id").equalTo(df2.col("user_id"))).and(
				df1.col("advertiser_id").equalTo(df2.col("advertiser_id")))
				.and(df1.col("timestamp").gt(df2.col("timestamp")));
		String[] aListOfCol = new String[] { "advertiser_id", "user_id",
				"event_type" };

		DataFrame cachedDF = df1
				.join(df2, joinExpr, "inner")
				.select(df1.col("advertiser_id"), df1.col("user_id"),
						df1.col("event_type")).dropDuplicates(aListOfCol)
				.cache();
		DataFrame cachedDF1 = cachedDF
				.orderBy(cachedDF.col("advertiser_id"))
				.groupBy(cachedDF.col("advertiser_id"),
						cachedDF.col("event_type")).count();
		// cachedDF1.show();
		cachedDF1.write().format("com.databricks.spark.csv")
				.option("header", "true").save("resources/count_of_users.csv");
	}

	public static void generateCountOfEvents(DataFrame df1, DataFrame df2) {
		Column joinExpr = df1.col("advertiser_id").equalTo(
				df2.col("advertiser_id"));
		DataFrame cachedDF = df1.join(df2, joinExpr, "outer")
				.select(df1.col("advertiser_id"), df1.col("event_type"))
				.cache();
		DataFrame cachedDF1 = cachedDF
				.orderBy(cachedDF.col("advertiser_id"))
				.groupBy(cachedDF.col("advertiser_id"),
						cachedDF.col("event_type")).count();
		// cachedDF1.show();
		cachedDF1.write().format("com.databricks.spark.csv")
				.option("header", "true").save("resources/count_of_events.csv");
	}

}
