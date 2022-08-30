package com.sniff.connect;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.dataproc.JsonValueStreamOp;
import com.alibaba.alink.operator.stream.source.KafkaSourceStreamOp;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class KafkaSourceStreamOpTest {

	public static void testKafkaSourceStreamOp() throws Exception {
		StreamOperator <?> data = new KafkaSourceStreamOp()
			.setBootstrapServers("192.168.201.100:9092")
			.setTopic("test_kafka_alink")
			.setStartupMode("EARLIEST")
			.setGroupId("alink_group");
//		DataStream<Row> dataStream = data.getDataStream();

//		dataStream.flatMap(new FlatMapFunction<Row, Object>() {
//			@Override
//			public void flatMap(Row value, Collector<Object> out) throws Exception {
//				System.out.println(value.getField(1));
//			}
//		});

		StreamOperator daddta = data
				.link(
						new JsonValueStreamOp()
								.setSelectedCol("message")
								.setReservedCols(new String[] {})
								.setOutputCols(
										new String[] {"ID", "name", "age", "sex", "like_"})
								.setJsonPath(new String[] {"$.ID", "$.name", "$.age", "$.sex",
										"$.like_"})
				);

		System.out.print(data.getSchema());


	}

	public static void main(String[] args) throws Exception {

		testKafkaSourceStreamOp();
		StreamOperator.execute();

	}
}