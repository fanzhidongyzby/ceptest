package com.test.florian.cep;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by zhidong.fzd on 17/3/2.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final SingleOutputStreamOperator<Event> source = env.setParallelism(1)
            .addSource(new SourceFunction<Event>() {
                @Override
                public void run(SourceContext<Event> sourceContext) throws Exception {
                    while (true) {
                        final Event event = Event.createEvent();
                        if (event.getValue() <= 5)
                            continue;
                        sourceContext.collect(event);
                        System.out.println("create event = " + event);
                        Thread.sleep(10);
                    }
                }

                @Override
                public void cancel() {

                }
            }).assignTimestampsAndWatermarks(new IngestionTimeExtractor<Event>());

        Pattern<Event, ?> warningPattern = Pattern.<Event> begin("first")
            .where(new FilterFunction<Event>() {
                @Override
                public boolean filter(Event monitorEvent) throws Exception {
                    return monitorEvent.getValue() > 5;
                }
            }).next("second").where(new FilterFunction<Event>() {
                @Override
                public boolean filter(Event monitorEvent) throws Exception {
                    return monitorEvent.getValue() > 5;
                }
            }).within(Time.seconds(5));

        PatternStream<Event> warningStream = CEP.pattern(source, warningPattern);

        DataStream<Event> warnings = warningStream
            .flatSelect(new PatternFlatSelectFunction<Event, Event>() {

                @Override
                public void flatSelect(Map<String, Event> pattern,
                                       Collector<Event> collector) throws Exception {
                    final Event first = pattern.get("first");
                    final Event second = pattern.get("second");

                    if (first.getValue() < second.getValue()) {
                        collector.collect(second);
                        System.out
                            .println("warning: " + first.getValue() + " -> " + second.getValue());
                    }
                }
            });

        Pattern<Event, ?> alertPattern = Pattern.<Event> begin("first").next("second")
            .within(Time.seconds(15));

        PatternStream<Event> alertStream = CEP.pattern(warnings, alertPattern);

        final DataStream<Event> alerts = alertStream
            .flatSelect(new PatternFlatSelectFunction<Event, Event>() {

                @Override
                public void flatSelect(Map<String, Event> pattern,
                                       Collector<Event> collector) throws Exception {
                    final Event first = pattern.get("first");
                    final Event second = pattern.get("second");

                    if (first.getValue() < second.getValue()) {
                        collector.collect(second);
                        System.out
                            .println("alert: " + first.getValue() + " -> " + second.getValue());
                    }
                }
            });

        //        alerts.print();

        env.execute();

    }
}
