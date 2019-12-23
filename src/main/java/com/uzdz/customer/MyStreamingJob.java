package com.uzdz.customer;

import com.google.common.base.Splitter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyStreamingJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.socketTextStream("localhost", 8888).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, String>> map = source.map(new MapFunction<String, List<String>>() {
            @Override
            public List<String> map(String value) throws Exception {
                List<String> strings = Splitter.on(",").trimResults().splitToList(String.valueOf(value));

                List<String> objects = new ArrayList<>();
                objects.addAll(strings);
                return objects;
            }
        }).filter(new FilterFunction<List<String>>() {
            @Override
            public boolean filter(List<String> value) throws Exception {
                if (!value.isEmpty() && value.size() >= 2) {
                    return true;
                }
                return false;
            }
        }).map(new MapFunction<List<String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(List<String> value) throws Exception {
                return new Tuple2<String, String>(value.get(0), value.get(1));
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> tuple2SingleOutputStreamOperator = map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {

            Long bound = 10000L;

            Long maxTs = 0L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTs - bound);
            }

            @Override
            public long extractTimestamp(Tuple2<String, String> element, long previousElementTimestamp) {
                maxTs = maxTs > Long.parseLong(element.f1) ? maxTs : Long.parseLong(element.f1);
                System.out.println("maxTs:" + maxTs + "- element:" + element.f1);
                return Long.parseLong(element.f1);
            }
        }).setParallelism(1);

        tuple2SingleOutputStreamOperator.keyBy(value -> value.f0).timeWindow(Time.seconds(10)).process(new MyProcessWindowFunction()).setParallelism(1);

        env.execute("MyStreamingJob");
    }

    public static  class MyProcessWindowFunction
            extends ProcessWindowFunction<Tuple2<String, String>, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, String>> elements, Collector<String> out) throws Exception {
            System.out.println(s + "~~~~~~~~~~~~~~~~~~~~~~~");

            System.out.println("分组Key：" + s);

            for (Tuple2<String, String> data: elements) {

                System.out.println("当前批次数据：" + data);

                out.collect(data.f0 + "-" + data.f1);
            }

            System.out.println(s + "~~~~~~~~~~~~~~~~~~~~~~~");
        }
    }
}

