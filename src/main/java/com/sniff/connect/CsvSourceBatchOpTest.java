package com.sniff.connect;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.TextSimilarityPairwise;
import org.junit.Test;

public class CsvSourceBatchOpTest {

	@Test
	public void testCsvSourceBatchOp() throws Exception {
		String filePath = "/Users/bianshaojie/Desktop/error2.csv";
		String schema
			= "product_code string,title string, similar_product_code string,similar_product_title string,flag string";
		CsvSourceBatchOp csvSource = new CsvSourceBatchOp()
			.setFilePath(filePath)
			.setSchemaStr(schema)
			.setFieldDelimiter(",");
		csvSource.print();
		TextSimilarityPairwise similarity = new TextSimilarityPairwise().setSelectedCols("title", "similar_product_title").setMetric(
				"LEVENSHTEIN").setOutputCol("output");
		similarity.transform(csvSource).print();
	}
}