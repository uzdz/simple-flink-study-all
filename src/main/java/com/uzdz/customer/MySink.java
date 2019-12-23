package com.uzdz.customer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MySink extends RichSinkFunction {

    @Override
    public void open(Configuration parameters) throws Exception {

        System.out.println("打开一些连接");
        super.open(parameters);
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {

        System.out.println("Sink输出，当前元素：" + value);
    }

    @Override
    public void close() throws Exception {

        System.out.println("关闭一些连接");
        super.close();
    }
}
