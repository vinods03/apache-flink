package p1;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;


public class TrackLargeChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {

	private double threshold;
	private transient ValueState<Double> prevWindowMaxTrade;
	
	public TrackLargeChange(double threshold) {
		this.threshold = threshold;
	}
	
	public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
		
		Double prevMaxTradePrice = prevWindowMaxTrade.value();
		Double currMaxTradePrice = 0.0;
		String currMaxTimestamp = "";
		
		for (Tuple5<String, String, String, Double, Integer> element : input) {
			if (element.f3 > currMaxTradePrice) {
				currMaxTradePrice = element.f3;
				currMaxTimestamp = element.f0 + ":" + element.f1;
			}
		}
		
		Double maxTradePriceChange = ((currMaxTradePrice - prevMaxTradePrice ) / (currMaxTradePrice)) * 100;
		
		if ( prevMaxTradePrice != 0 &&  // don't calculate delta the first time
	    		Math.abs((currMaxTradePrice - prevMaxTradePrice)/prevMaxTradePrice)*100 > threshold)
	    {
		out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " (" + prevMaxTradePrice + " - " + currMaxTradePrice + ") at  " + currMaxTimestamp);
	    }
	    prevWindowMaxTrade.update(currMaxTradePrice);
		
	}
	
	public void open(Configuration config) 
	{
	    ValueStateDescriptor<Double> descriptor =  new ValueStateDescriptor<Double>( "prev_max",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0);
	    prevWindowMaxTrade = getRuntimeContext().getState(descriptor);
	} 
}
