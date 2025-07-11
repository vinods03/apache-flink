package p1;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import java.util.stream.Collectors;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;


public class TrackChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {

	private transient ValueState<Double> prevWindowMaxTrade;
	private transient ValueState<Integer> prevWindowMaxVol;
	
	public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
		
		String windowStart = "";
		String windowEnd = "";
		
		Double windowMaxTrade = 0.0;
		Double windowMinTrade = 0.0;
		
		Integer windowMaxVol = 0;
		Integer windowMinVol = 0;
		
		Double maxTradeChange = 0.0;
	    Double maxVolChange = 0.0;
		
		for(Tuple5<String, String, String, Double, Integer> element : input) {
			
			if (windowStart.isEmpty()) {
				windowStart = element.f0 + " " + element.f1;
				windowMinTrade = element.f3;
				windowMinVol = element.f4;
			}
			
			if (element.f3 > windowMaxTrade) {
				windowMaxTrade = element.f3;
			}
			
			if (element.f3 < windowMinTrade) {
				windowMinTrade = element.f3;
			}
			
			if (element.f4 > windowMaxVol) {
				windowMaxVol = element.f4;
			}
			
			if (element.f4 < windowMinVol) {
				windowMinVol = element.f4;
			}
			
			windowEnd = element.f0 + " " + element.f1;
		}
		
		if (prevWindowMaxTrade.value() != 0) {
			maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
		}
		
		if (prevWindowMaxVol.value() != 0) {
			maxVolChange = ((windowMaxVol - prevWindowMaxVol.value())*1.0 / prevWindowMaxVol.value()) * 100;
		}
		
		 prevWindowMaxTrade.update(windowMaxTrade);
		 
		 prevWindowMaxVol.update(windowMaxVol);
		 
		 out.collect(windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange)
 		+ ", " +	 windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange));
		 
//		 String change = windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange)
//	 		+ ", " +	 windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange);	 
		 
	}
	
	public void open(Configuration config) 
	{
	   prevWindowMaxTrade = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev_max_trade",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0));
		    
	   prevWindowMaxVol = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("prev_max_vol",BasicTypeInfo.INT_TYPE_INFO, 0));
	} 

	
}
	

