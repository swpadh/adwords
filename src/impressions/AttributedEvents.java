package impressions;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class AttributedEvents {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf(true).setAppName("adwords")
				.setMaster("local[*]");
		SparkContext sc = new SparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(sc);

		DataFrame df1 = ReadEvents.getEventsDataFrame(sqlContext);
		DataFrame df2 = ReadImpressions.getImpressionsDataFrame(sqlContext);
		GenerateReports.generateCountOfUsers(df1, df2);
		GenerateReports.generateCountOfEvents(df1, df2);
		 GenerateStat.generateAttributedEventsStat(df1, df2);
	}

}
