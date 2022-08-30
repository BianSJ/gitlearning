package com.sniff.connect;

import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;


public class CsvSourceStreamOpTest {

	public  StreamOperator testCsvSourceStreamOp() throws Exception {
		String filePath = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
		String schema
			= "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
		StreamOperator <?> csvSource = new CsvSourceStreamOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setFieldDelimiter(",");

		return csvSource;
	}

}