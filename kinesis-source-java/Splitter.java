package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Splitter implements MapFunction<String, Tuple5<String, String, String, Double, Integer>> {
	
	public Tuple5<String, String, String, Double, Integer> map(String value) {
		
		String[] words = value.split(",");
		return new Tuple5<String, String, String, Double, Integer>(words[0], words[1], "XYZ", Double.parseDouble(words[2]), Integer.parseInt(words[3]));
		
	}

}
