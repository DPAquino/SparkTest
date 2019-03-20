package test.douglas.classes;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class ClsError404PerDay extends Configuries {
	private static final Logger LOG = LoggerFactory.getLogger(ClsError404PerDay.class);

	public ClsError404PerDay() {
	}

	@Override
	public void doResearch(JavaRDD<String> fullLog) {
		StringBuilder sbResult = new StringBuilder();
		sbResult.append("4. Quantidade de erros 404 por dia. \r\n");

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

		JavaPairRDD<String, String> agroup = fullLog.mapToPair(keyData);
		JavaPairRDD<String, String> errors = agroup.reduceByKey(func);
		List<Tuple2<String, String>> errorList = errors.collect();

		if (errorList.size() > 0) {
			for (Tuple2<String, String> h : errorList) {
				sbResult.append("Date " + h._1 + " : " + h._2 + " errors\r\n");
			}
			printResult(sbResult.toString(), ".\\Out\\test4.txt");
			LOG.info("Error 404 Research Finished");
		} else {
			sbResult.append("Dados não encontrados");
		}

	}

}
