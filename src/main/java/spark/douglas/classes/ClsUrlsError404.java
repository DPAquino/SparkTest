package spark.douglas.classes;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class ClsUrlsError404 extends Configuries {
	private static final Logger LOG = LoggerFactory.getLogger(ClsUrlsError404.class);

	public ClsUrlsError404() {
	}

	@Override
	public void doResearch(JavaRDD<String> fullLog) {
		StringBuilder sbResult = new StringBuilder();
		sbResult.append("3. Os 5 URLs que mais causaram erro 404. \r\n");

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

		JavaPairRDD<String, String> agroup = fullLog.mapToPair(keyData);
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

		if (errorHash.size() > 0) {
			for (int i = 0; i < 5; i++) {
				Integer key = getMaxKey(errorHash);
				sbResult.append(key + " errors 404 - to " + errorHash.get(key) + "\r\n");
				errorHash.remove(key);
			}
			printResult(sbResult.toString(), ".\\Out\\test3.txt");
			LOG.info("Top Five Error 404 Research Finished");
		} else {
			sbResult.append("Dados não encontrados");
		}
	}

	private Integer getMaxKey(HashMap<Integer, String> errorHash) {
		Integer key = Collections.max(errorHash.entrySet(), Map.Entry.comparingByKey()).getKey();
		return key;
	}

}
