package p1;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

public class TotalTradeInEachWindow extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {

	public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
		
		String windowStart = "";
		String windowEnd = "";
		Double windowSumTrade = 0.0;	
		Integer windowSumVol = 0;
		
		for (Tuple5<String, String, String, Double, Integer> element: input ) {
			
			if (windowStart.isEmpty()) {
				windowStart = element.f0 + " " + element.f1;
			}
			
			windowSumTrade = windowSumTrade + element.f3;
			windowSumVol = windowSumVol + element.f4;
			
			windowEnd = element.f0 + " " + element.f1;
					
		}
		
		out.collect("Sum of Trades in the window " +windowStart +" - " +windowEnd +" is: " +windowSumTrade +" and the total volume of trades is: " +windowSumVol);
		
		windowSumTrade = 0.0;
		windowSumVol = 0;
		
		
	}
}
