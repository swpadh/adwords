package impressions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class GenerateStat {
	private static final List<EventsReport> eventsReportLst = new ArrayList<EventsReport>();

	public static void generateAttributedEventsStat(DataFrame df1, DataFrame df2) {
		Column joinExpr = (df1.col("user_id").equalTo(df2.col("user_id"))).and(
				df1.col("advertiser_id").equalTo(df2.col("advertiser_id")))
				.and(df1.col("timestamp").gt(df2.col("timestamp")));
		String[] aListOfCol = new String[] { "advertiser_id", "user_id",
				"event_type" };

		DataFrame cachedDF = df1
				.join(df2, joinExpr, "inner")
				.select(df1.col("advertiser_id"), df1.col("user_id"),
						df1.col("event_type"), df1.col("timestamp"))
				.dropDuplicates(aListOfCol).cache();

		DataFrame cachedDF1 = cachedDF.orderBy(cachedDF.col("advertiser_id"),
				cachedDF.col("timestamp"));

		JavaRDD<Row> attrEvents = cachedDF1.toJavaRDD();
		attrEvents.foreach(new VoidFunction<Row>() {
			int sum = 0;

			@Override
			public void call(Row aRow) throws Exception {
				if (aRow.length() >= 4) {
					addReport(aRow, eventsReportLst, ++sum);
				}
				// System.out.println(aRow.mkString(","));
			}
		});

		String[] aListOfAdv = new String[] { "advertiser_id" };

		DataFrame cachedDF2 = df1.join(df2, joinExpr, "outer")
				.select(df1.col("advertiser_id")).dropDuplicates(aListOfAdv)
				.cache();

		JavaRDD<Row> nonAttrEvents = cachedDF2.toJavaRDD();
		nonAttrEvents.foreach(new VoidFunction<Row>() {

			@Override
			public void call(Row aRow) throws Exception {
				if (aRow.length() >= 1) {
					addAdvertiser(aRow, eventsReportLst);
				}
			}
		});
		printReport(eventsReportLst);
	}

	private static void addReport(Row aRow, List<EventsReport> eventsReportLst,
			int sum) {
		if(aRow.isNullAt(0))
			return;		
		String advertiserId = String.valueOf(aRow.getInt(0));
		boolean found = false;
		for (EventsReport evReport : eventsReportLst) {
			if (evReport.getAdvertiserId().equals(advertiserId)) {
				evReport.setUserId(aRow.getString(1));
				evReport.setEventCount(sum);
				found = true;
				break;
			}
		}
		if (!found) {
			eventsReportLst.add(new EventsReport(
					String.valueOf(aRow.getInt(0)), sum, aRow.getString(1)));
		}
	}

	private static void addAdvertiser(Row aRow,
			List<EventsReport> eventsReportLst) {
		if(aRow.isNullAt(0))
			return;	
		String advertiserId = String.valueOf(aRow.getInt(0));
		boolean found = false;
		for (EventsReport evReport : eventsReportLst) {
			if (evReport.getAdvertiserId().equals(advertiserId)) {
				found = true;
				break;
			}
		}
		if (!found) {
			eventsReportLst
					.add(new EventsReport(String.valueOf(aRow.getInt(0))));
		}
	}

	private static void printReport(List<EventsReport> eventsReportLst) {
		for (EventsReport rept : eventsReportLst) {
			StringBuilder strBuilder = new StringBuilder(" Advertiser "
					+ rept.getAdvertiserId());
			strBuilder.append(" : ");
			strBuilder.append(rept.getEventCount());
			strBuilder.append(" attributed event, ");
			strBuilder.append(rept.getUserId().size());
			if (rept.getUserId().size() > 0) {
				strBuilder.append(" unique user ");
			} else {
				strBuilder
						.append(" unique user that has generated an attributed");
			}
			System.out.println(strBuilder.toString());
		}
	}
}
