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


public class TrackLargeDelta extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {

	private transient ValueState<Double> prevWindowMaxTrade;
	private double threshold;
	
	public TrackLargeDelta(double threshold) {
		this.threshold = threshold;
	}
	
	public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
	
	String windowStart = "";
	String windowEnd = "";
	
	Double windowMaxTrade = 0.0;
	Double windowMinTrade = 0.0;
	
	Integer windowMaxVol = 0;
	Integer windowMinVol = 0;
	
	Double maxTradeChange = 0.0;
    Double maxVolChange = 0.0;
    
//    Double prevMaxTradePrice = prevWindowMaxTrade.value();
    String windowMaxTimestamp = "";
    
    for (Tuple5<String, String, String, Double, Integer> element: input) {
    	
    	if (windowStart.isEmpty()) {
    		
//    		System.out.println("Window Start is empty");
			windowStart = element.f0 + " " + element.f1;
			windowMinTrade = element.f3;
			windowMinVol = element.f4;
		}
    	
    	if (element.f3 > windowMaxTrade) {
			windowMaxTrade = element.f3;
			windowMaxTimestamp = element.f0 + ":" + element.f1;
			
		}
    	
    	windowEnd = element.f0 + " " + element.f1;
    }
    
    if (prevWindowMaxTrade.value() != 0) {
		maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
		if (Math.abs(maxTradeChange) > threshold) {
			out.collect("Large Change Detected of " + String.format("%.2f", maxTradeChange) + "%" + " (" + prevWindowMaxTrade.value() + " - " + windowMaxTrade + ") at  " + windowMaxTimestamp);
		}
	}
    
    prevWindowMaxTrade.update(windowMaxTrade);
    
}
	
	public void open(Configuration config) 
	{
   
		prevWindowMaxTrade = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev_max_trade",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0));
	} 
	
}
