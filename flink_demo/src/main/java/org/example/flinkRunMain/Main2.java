package org.example.flinkRunMain;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.source.SourceFromMySQL;

/**
 * @Description
 * @Author: harden
 * @Date: 2022-12-29 14:54
 **/
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMySQL()).print();

        env.execute("Flink add data sourc");
    }
}
