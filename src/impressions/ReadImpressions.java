package impressions;

import java.util.HashMap;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadImpressions {
	public static DataFrame getImpressionsDataFrame(final SQLContext sqlContext)
	{
		StructType impressionsSchema = new StructType(new StructField[] {
			    new StructField("timestamp", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("advertiser_id", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("creative_id", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("user_id", DataTypes.StringType, true, Metadata.empty())
			});
		HashMap<String, String> impressionsOptions = new HashMap<String, String>();
		impressionsOptions.put("header", "false");
		impressionsOptions.put("inferSchema", "false");
		impressionsOptions.put("path", "resources/impressions.csv");
		
		DataFrame df = sqlContext.load("com.databricks.spark.csv", impressionsSchema, impressionsOptions);
		return df;
	}
}
