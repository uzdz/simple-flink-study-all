package com.uzdz.customer;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.util.concurrent.TimeUnit;

public class MySource extends RichSourceFunction<Integer> {

    /**
     * 模拟输入源
     */
    Integer i = 1;

    /**
     * 取消标识
     */
    boolean cancel = true;

    @Override
    public void run(SourceContext ctx) throws Exception {
        // 每个Task执行的逻辑
        while (cancel && i <= 10 ) {
            System.out.println("Flink镊取到元素：" + i);
            ctx.collect(i);
            i += 1;

            // 模拟数据流，沉睡1秒再向Flink输入元素
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        // 可以通过此方法取消源输入
        cancel = false;
    }
}
