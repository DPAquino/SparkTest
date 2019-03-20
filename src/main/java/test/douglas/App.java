package test.douglas;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import test.douglas.classes.ClsBytesReturned;
import test.douglas.classes.ClsError404;
import test.douglas.classes.ClsError404PerDay;
import test.douglas.classes.ClsUniqueHost;
import test.douglas.classes.ClsUrlsError404;

public class App {
	private static JavaSparkContext ctx;

	public static void main(String[] args) {

		ctx = getContext();

		JavaRDD<String> logJul = getAccesslogFileJul();
		JavaRDD<String> logAug = getAccesslogFileAug();
		JavaRDD<String> fullLog = logJul.union(logAug);

		uniqueHosts(fullLog);
		errors404(fullLog);
		urlsWhithError404(fullLog);
		errors404PerDay(fullLog);
		bytesReturned(fullLog);

		ctx.close();
	}

	private static void bytesReturned(JavaRDD<String> fullLog) {
		ClsBytesReturned bytesReturned = new ClsBytesReturned();
		bytesReturned.doResearch(fullLog);
	}

	private static void errors404PerDay(JavaRDD<String> fullLog) {
		ClsError404PerDay error404PerDay = new ClsError404PerDay();
		error404PerDay.doResearch(fullLog);
	}

	private static void urlsWhithError404(JavaRDD<String> fullLog) {
		ClsUrlsError404 urlsError404 = new ClsUrlsError404();
		urlsError404.doResearch(fullLog);
	}

	private static void errors404(JavaRDD<String> fullLog) {
		ClsError404 error404 = new ClsError404();
		error404.doResearch(fullLog);
	}

	private static void uniqueHosts(JavaRDD<String> fullLog) {
		ClsUniqueHost uniqueHost = new ClsUniqueHost();
		uniqueHost.doResearch(fullLog);
	}

	private static JavaRDD<String> getAccesslogFileJul() {
		return ctx.textFile(".\\NASA_access_log_Jul95\\access_log_Jul95");
	}

	private static JavaRDD<String> getAccesslogFileAug() {
		return ctx.textFile(".\\NASA_access_log_Aug95\\access_log_Aug95");
	}

	private static JavaSparkContext getContext() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
		return new JavaSparkContext(conf);
	}
}
