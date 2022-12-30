package org.example.flinkRunMain;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.example.entity.Student;
import org.example.source.SinkToMySQL;

import java.util.Properties;

/**
 * @Description 里的 source 是从 kafka 读取数据的，然后 Flink 从 Kafka 读取到数据（JSON）
 * 后用阿里 fastjson 来解析成 student 对象，然后在 addSink 中使用我们创建的 SinkToMySQL，
 * 这样就可以把数据存储到 MySQL 了
 * @Author: harden
 * @Date: 2022-12-30 14:30
 **/
public class Main3 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象

        /*1. 自电定义Data Sink将kafka接受的数据转到mysql中*/
        /*student.addSink(new SinkToMySQL()); //数据 sink 到 mysql*/

        /*2. Flink Data Transformation,数据转换操作
        *   将每个人的年龄+5 */
       /* SingleOutputStreamOperator<Student> map = student.map((student1)->{
            Student s2 = new Student();
            s2.id=student1.id;
            s2.name = student1.name;
            s2.password = student1.password;
            s2.age = student1.age + 5;
            return s2;
        });
        map.print();*/

        SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student value, Collector<Student> out) throws Exception {
                if (value.id % 2 == 0) {
                    out.collect(value);
                }
            }
        });
        flatMap.print();
        env.execute("Flink add sink");

    }
}
