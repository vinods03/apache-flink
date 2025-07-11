package p1;

//import java.sql.Timestamp;
//import java.text.SimpleDateFormat;


//import org.apache.flink.util.Collector;
//import org.apache.flink.configuration.Configuration;
//
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

//import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;

//import java.text.SimpleDateFormat;
//import java.sql.Timestamp;
//import org.apache.flink.streaming.util.serialization.DeserializationSchema;
//import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

public class Stock {
	
//	private static final String region = "us-east-1";
//	private static final String inputStreamName = "vinod-apache-flink-kinesis-stream";
	
//	private static DataStream<String> createSourceFromConfig(StreamExecutionEnvironment env) {
//		 Properties inputProperties = new Properties();
//	     inputProperties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
//	     inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");
//	     return env.addSource(new FlinkKinesisConsumer<>(inputStreamName, new SimpleStringSchema(), inputProperties));
//	}

	public static void main(String[] args) throws Exception {
		
			
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
//		Method 1 of setting the properties
//		not good because this is expecting access key / secret access key
		
		Properties consumerConfig = new Properties();
		consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
		consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "<>");
		consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "<>");
		consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

		DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
		    "vinod-apache-flink-kinesis-stream", new SimpleStringSchema(), consumerConfig));
		
//		DataStream<Tuple5<String, String, String, Double, Integer>> data = kinesis
//		.map(new Splitter());
		
		DataStream<Tuple5<String, String, String, Double, Integer>> data = kinesis
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
		
//		can use this when running on an EC2 instance
//		data.writeAsText("/home/ubuntu/rawdata.txt");
		
//		use this when running from AWS Console - flink application
//		data.writeAsText("s3://vinod-apache-flink/kinesis-based/raw_output",org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
		
//		compute per minute-window stats

		DataStream<String> each_window_stats = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
			{
				public String getKey(Tuple5<String, String, String, Double, Integer> value) {
				return value.f2;
				}

			}

		)
				.window(TumblingEventTimeWindows.of(Time.minutes(1)))
				.process(new TrackChange());

		
		each_window_stats.writeAsText("s3://vinod-apache-flink/kinesis-based/each_window_stats");
		
//		across windows - max trade amount change beyond threshold
		
		DataStream<String> across_windows_large_change_in_tradeprice = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
		{
			public String getKey(Tuple5<String, String, String, Double, Integer> value) {
				return value.f2;
			}
		  }
	   )
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
	            .process(new TrackLargeDelta(5));
		
		across_windows_large_change_in_tradeprice.writeAsText("s3://vinod-apache-flink/kinesis-based/across_windows_large_change_in_tradeprice");
		
		DataStream<String> total_trade_in_each_window = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>()
				{
					public String getKey(Tuple5<String, String, String, Double, Integer> value) {
					return value.f2;
					}
				}
		     )
		   .window(TumblingEventTimeWindows.of(Time.minutes(5)))
		   .process(new TotalTradeInEachWindow());
		
		total_trade_in_each_window.writeAsText("s3://vinod-apache-flink/kinesis-based/total_trade_in_each_window");
               
		env.execute("Kinesis Consumer Example");

	}

}
