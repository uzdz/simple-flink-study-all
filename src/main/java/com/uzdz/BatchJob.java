/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uzdz;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> txt = env.readTextFile("/Users/uzdz/work/simple-flink-study-all/flink.txt");

        txt.flatMap(new RichFlatMapFunction<String, Tuple3<String, Integer, String>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                IntCounter intCounter = new IntCounter();
                getRuntimeContext().addAccumulator("uzdz", intCounter);
            }

            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, String>> collector) throws Exception {
                // 统一大小写并把每一行切割为单词
                String[] tokens = value.toLowerCase().split(" ");

                getRuntimeContext().getAccumulator("uzdz").add(1);

                // 消费二元组
                for (String token : tokens) {
                    if (token.length() > 0) {
                    	Time.milliseconds(100);
                        collector.collect(new Tuple3<String, Integer, String>(token, 1, String.valueOf(System.currentTimeMillis())));
                    }
                }
            }
        }).distinct(0)
				.writeAsText("/Users/uzdz/work/simple-flink-study-all/output-flink.txt", FileSystem.WriteMode.OVERWRITE)
				.setParallelism(1);


		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */

		// execute program
        JobExecutionResult result = env.execute("Flink Batch Java API Skeleton");
        System.out.println("总共行数：" + result.getAccumulatorResult("uzdz"));;

    }
}
