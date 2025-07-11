package p1;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.flink.api.java.utils.ParameterTool;

public class Stock {

	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		final ParameterTool params = ParameterTool.fromArgs(args);
		env.getConfig().setGlobalJobParameters(params);
		
//		read the data, assign timestamps and watermarks
		DataStream<Tuple5<String, String, String, Double, Integer>> data = env.readTextFile("/home/ubuntu/FUTURES_TRADES.txt")
				.map(new Splitter())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<String, String, String, Double, Integer>>()
				{
					    private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                        public long extractAscendingTimestamp(Tuple5<String, String, String, Double, Integer> value)
					    {
							try
							{
						    Timestamp ts = new Timestamp(sdf.parse(value.f0 + " " + value.f1).getTime());
						    return ts.getTime();
							} 
							catch(Exception e)
							{
						    throw new java.lang.RuntimeException("Timestamp Parsing Error");
							}  
						}
					}
				);
		
//		compute per minute-window stats
		DataStream<String> change = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
			           public String getKey(Tuple5<String, String, String, Double, Integer> value) {
			        	   return value.f2;
			           }
				}
			)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new TrackChange());
		
		change.writeAsText("/home/ubuntu/Changes.txt");
		
		
//		alert when price change from one window to another exceeds threshold
		
		DataStream<String> largeDelta = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
					public String getKey(Tuple5<String, String, String, Double, Integer> value) {
						return value.f2;
					}
				}
		)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.process(new TrackLargeChange(5));
		
		largeDelta.writeAsText("/home/ubuntu/Alert.txt");
		
//		using a slightly different function - alert when price change from one window to another exceeds threshold
		
		DataStream<String> largeDeltaAnother = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
					public String getKey(Tuple5<String, String, String, Double, Integer> value) {
						return value.f2;
					}
				}
		)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.process(new TrackLargeDelta(5));
		
		largeDeltaAnother.writeAsText("/home/ubuntu/AlertAnother.txt");
		
//		sum of trades and volume within a window - so far, we have checked across windows
		
		DataStream<String> totals = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
					public String getKey(Tuple5<String, String, String, Double, Integer> value) {
						return value.f2;
					}
				}
		
			)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new TotalTrade());
		
		totals.writeAsText("/home/ubuntu/totals.txt");
		
//		average trade volume change greater than threshold
		
		DataStream<String> largeChangeInAverageTrade = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
					public String getKey(Tuple5<String, String, String, Double, Integer> value) {
						return value.f2;
					}
				}
				
				).window(TumblingEventTimeWindows.of(Time.minutes(Integer.parseInt(params.get("timewindow")))))
				.process(new AverageTradeChange(Double.parseDouble(params.get("threshold"))));
		
		largeChangeInAverageTrade.writeAsText("/home/ubuntu/largechangesinaverage.txt");
		
		env.execute("Stock Analysis");

	}

}
