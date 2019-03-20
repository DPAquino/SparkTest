package test.douglas.classes;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class ClsUniqueHost extends Configuries {

	private static final Logger LOG = LoggerFactory.getLogger(ClsUniqueHost.class);

	public ClsUniqueHost() {
	}

	@Override
	public void doResearch(JavaRDD<String> fullLog) {
		StringBuilder sbResult = new StringBuilder();
		sbResult.append("1. Número de hosts únicos. \r\n");

		JavaPairRDD<String, Integer> agroup = fullLog.mapToPair(s -> new Tuple2<String, Integer>(s.split(" ")[0], 1));
		JavaPairRDD<String, Integer> hostNumber = agroup.reduceByKey((x, y) -> x + y);
		List<Tuple2<String, Integer>> hostList = hostNumber.collect();

		if (hostList.size() > 0) {
			for (Tuple2<String, Integer> h : hostList) {
				if (h._2 == 1)
					sbResult.append("Unique Host: " + h._1 + "\r\n");
			}
			printResult(sbResult.toString(), ".\\Out\\test1.txt");
			LOG.info("Unique Hosts Research Finished");
		} else {
			sbResult.append("Dados não encontrados");
		}
	}

}
