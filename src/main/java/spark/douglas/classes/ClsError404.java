package spark.douglas.classes;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class ClsError404 extends Configuries {
	private static final Logger LOG = LoggerFactory.getLogger(ClsError404.class);

	public ClsError404() {
	}

	@Override
	public void doResearch(JavaRDD<String> fullLog) {
		StringBuilder sbResult = new StringBuilder();
		sbResult.append("2. O total de erros 404. \r\n");

		PairFunction<String, String, Integer> keyData = new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String x) {
				String[] splitted = x.split(" ");
				String tuple2Value = splitted[splitted.length - 2];

				return new Tuple2(tuple2Value, 1);
			}
		};

		JavaPairRDD<String, Integer> agroup = fullLog.mapToPair(keyData);
		JavaPairRDD<String, Integer> errors = agroup.reduceByKey((x, y) -> x + y);
		List<Tuple2<String, Integer>> errorList = errors.collect();
		if (errorList.size() > 0) {
			for (Tuple2<String, Integer> h : errorList) {
				if (h._1.equals("404"))
					sbResult.append("Errors 404: " + h._2 + "\r\n");
			}
			printResult(sbResult.toString(), ".\\Out\\test2.txt");
			LOG.info("Error 404 Research Finished");
		} else {
			sbResult.append("Dados não encontrados");
		}
	}
}
