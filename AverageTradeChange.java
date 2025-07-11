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

public class AverageTradeChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
	
	private transient ValueState<Double> prevWindowAvgTrade;
	private double threshold;
	
	public AverageTradeChange(double threshold) {
		this.threshold = threshold;
	}
	
	public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
		
		String windowStart = "";
		String windowEnd = "";
		Double windowSumTrade = 0.0;
		Integer windowSumVol = 0;
		Double windowAvgTrade = 0.0;
		Double avgTradeChange = 0.0;
		
		for(Tuple5<String, String, String, Double, Integer> element: input) {
			
			if (windowStart.isEmpty()) {
				windowStart = element.f0 + " " + element.f1;
			}
			
			windowSumTrade = windowSumTrade + element.f3;
			windowSumVol = windowSumVol + element.f4;
			windowEnd = element.f0 + " " + element.f1;
		}
		
		windowAvgTrade = windowSumTrade / windowSumVol;
		
		if (prevWindowAvgTrade.value() != 0) {
			
//			maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
			avgTradeChange = (windowAvgTrade - prevWindowAvgTrade.value())/(prevWindowAvgTrade.value()) * 100;
			if (Math.abs(avgTradeChange) > threshold) {
				out.collect("Between " +windowStart + " and " +windowEnd + " the average trade value changed by " +avgTradeChange + "%" );
				
			}
		}
		
		prevWindowAvgTrade.update(windowAvgTrade);
		windowSumTrade = 0.0;
		windowSumVol = 0;
	}
	
	public void open(Configuration config) 
	{
   
		prevWindowAvgTrade = getRuntimeContext().getState(new ValueStateDescriptor<Double>("prev_avg_trade",BasicTypeInfo.DOUBLE_TYPE_INFO, 0.0));
	}

	

}
