package spark.douglas.classes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.spark.api.java.JavaRDD;

public abstract class Configuries implements java.io.Serializable {
	public abstract void doResearch(JavaRDD<String> fullLog);
	public void printResult(String result, String path) {
		if (result.length() == 0)
			result = "Nenhum resultado para esta pesquisa!";
		try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(path))) {
			writer.write(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
