package impressions;

import java.util.HashMap;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadEvents {
	
	public static  DataFrame getEventsDataFrame(final SQLContext sqlContext)
	{
		StructType eventsSchema = new StructType(new StructField[] {
			    new StructField("timestamp", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("event_id", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("advertiser_id", DataTypes.IntegerType, true, Metadata.empty()),
			    new StructField("user_id", DataTypes.StringType, true, Metadata.empty()),
			    new StructField("event_type", DataTypes.StringType, true, Metadata.empty())
			});
		HashMap<String, String> eventsOptions = new HashMap<String, String>();
		eventsOptions.put("header", "false");
		eventsOptions.put("inferSchema", "false");
		eventsOptions.put("path", "resources/events.csv");
		
		DataFrame df = sqlContext.load("com.databricks.spark.csv",eventsSchema, eventsOptions);
		return df;
	}

}
