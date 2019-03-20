package spark.douglas.classes;

import java.util.List;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class ClsBytesReturned extends Configuries {
	private static final Logger LOG = LoggerFactory.getLogger(ClsBytesReturned.class);

	public ClsBytesReturned() {
	}

	@Override
	public void doResearch(JavaRDD<String> fullLog) {
		StringBuilder sbResult = new StringBuilder();
		sbResult.append("5. O total de bytes retornados. \r\n");

		long sum = 0;
		JavaPairRDD<String, Integer> agroup = fullLog
				.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[s.split(" ").length - 1], 1));
		List<Tuple2<String, Integer>> bytesList = agroup.collect();

		for (Tuple2<String, Integer> h : bytesList) {
			if (NumberUtils.isNumber(h._1))
				sum += Long.valueOf(h._1);
		}

		sbResult.append("Total Bytes: " + sum);
		printResult(sbResult.toString(), ".\\Out\\test5.txt");
	}

}
