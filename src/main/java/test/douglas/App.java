package test.douglas;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class App {
	private static JavaSparkContext ctx;

	public static void main(String[] args) {

		ctx = getContext();

		JavaRDD<String> logJul = getAccesslogFileJul();
		JavaRDD<String> logAug = getAccesslogFileAug();		
		JavaRDD<String> testLog = logJul.union(logAug);

		uniqueHosts(testLog);
		errors404(testLog);
		urlsWhithError404(testLog);
		errors404PerDay(testLog);
		bytesReturned(testLog);

		ctx.close();
	}

	private static void bytesReturned(JavaRDD<String> testLog) {
		long sum = 0;
		JavaPairRDD<String, Integer> agroup = testLog
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[s.split(" ").length - 1], 1));
		List<Tuple2<String, Integer>> bytesList = agroup.collect();

		for (Tuple2<String, Integer> h : bytesList) {
			if (NumberUtils.isNumber(h._1))
				sum += Long.valueOf(h._1);
		}

		System.out.println("Total Bytes: " + sum);
	}

	private static void errors404PerDay(JavaRDD<String> testLog) {
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String x) {
				String[] splitted = x.split(" ");
				String tuple2Value = splitted[3].substring(1, 12);
				String code = splitted[splitted.length - 2];

				return new Tuple2(tuple2Value, code.equals("404") ? "1" : "");
			}
		};

		Function2<String, String, String> func = new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				Integer v1Int = null;
				Integer v2Int = null;
				if (v1.length() > 0)
					v1Int = Integer.valueOf(v1);
				if (v2.length() > 0)
					v2Int = Integer.valueOf(v2);
				if (v1Int != null && v2Int != null)
					return String.valueOf(v1Int + v2Int);

				return v1 + v2;
			}
		};

		JavaPairRDD<String, String> agroup = testLog.mapToPair(keyData);
		JavaPairRDD<String, String> errors = agroup.reduceByKey(func);
		List<Tuple2<String, String>> errorList = errors.collect();

		for (Tuple2<String, String> h : errorList) {
			System.out.println(h._1 + " : " + h._2);
		}

	}

	private static void urlsWhithError404(JavaRDD<String> testLog) {

		HashMap<Integer, String> errorHash = new HashMap<Integer, String>();
		Integer max = 0;
		PairFunction<String, String, String> keyData = new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String x) {
				String[] splitted = x.split(" ");
				String code = splitted[splitted.length - 2];
				String host = splitted[0];

				return new Tuple2(host, code.equals("404") ? "1" : "");
			}
		};

		Function2<String, String, String> func = new Function2<String, String, String>() {
			@Override
			public String call(String v1, String v2) throws Exception {
				Integer v1Int = null;
				Integer v2Int = null;
				if (v1.length() > 0)
					v1Int = Integer.valueOf(v1);
				if (v2.length() > 0)
					v2Int = Integer.valueOf(v2);
				if (v1Int != null && v2Int != null)
					return String.valueOf(v1Int + v2Int);

				return v1 + v2;
			}
		};

		JavaPairRDD<String, String> agroup = testLog.mapToPair(keyData);
		JavaPairRDD<String, String> errors = agroup.reduceByKey(func);

		List<Tuple2<String, String>> errorList = errors.collect();

		for (Tuple2<String, String> h : errorList) {
			if (!h._2.equals("")) {
				int amount = Integer.parseInt(h._2);
				if (amount > max) {
					errorHash.put(Integer.valueOf(h._2), h._1);
					max = amount;
				}
			}
		}

		for (int i = 0; i < 5; i++) {
			Integer key = getMaxKey(errorHash);
			System.out.println(key + " errors 404 - to " + errorHash.get(key));
			errorHash.remove(key);
		}
	}

	private static Integer getMaxKey(HashMap<Integer, String> errorHash) {
		Integer key = Collections.max(errorHash.entrySet(), Map.Entry.comparingByKey()).getKey();
		return key;
	}

	private static void errors404(JavaRDD<String> testLog) {

		PairFunction<String, String, Integer> keyData = new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String x) {
				String[] splitted = x.split(" ");
				String tuple2Value = splitted[splitted.length - 2];

				return new Tuple2(tuple2Value, 1);
			}
		};

		JavaPairRDD<String, Integer> agroup = testLog.mapToPair(keyData);
		JavaPairRDD<String, Integer> errors = agroup.reduceByKey((x, y) -> x + y);
		List<Tuple2<String, Integer>> errorList = errors.collect();

		for (Tuple2<String, Integer> h : errorList) {
			if (h._1.equals("404"))
				System.out.println("Error 404: " + h._2);
		}
	}

	private static void uniqueHosts(JavaRDD<String> testLog) {
		JavaPairRDD<String, Integer> agroup = testLog.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[0], 1));
		JavaPairRDD<String, Integer> hostNumber = agroup.reduceByKey((x, y) -> x + y);
		List<Tuple2<String, Integer>> hostList = hostNumber.collect();

		for (Tuple2<String, Integer> h : hostList) {
			if (h._2 == 1)
				System.out.println("unique host: " + h._1);
		}
	}

	private static JavaRDD<String> getAccesslogFileJul() {
		return ctx.textFile("..\\NASA_access_log_Jul95\\access_log_Jul95");
	}
	
	private static JavaRDD<String> getAccesslogFileAug() {
		return ctx.textFile("..\\NASA_access_log_Aug95\\access_log_Aug95");
	}

	private static JavaSparkContext getContext() {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
		return new JavaSparkContext(conf);
	}
}
