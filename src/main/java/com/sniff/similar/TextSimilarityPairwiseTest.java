package com.sniff.similar;

import org.apache.flink.types.Row;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.pipeline.similarity.TextSimilarityPairwise;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TextSimilarityPairwiseTest {
	@Test
	public void testTextSimilarityPairwise() throws Exception {
		List <Row> df = Arrays.asList(
			Row.of(0, "扁平足足弓鞋垫防痛半码硅胶高跟鞋七分隐形按摩透明支撑", "女"),
			Row.of(1, "a a c e d w", "a a b b e d"),
			Row.of(2, "c d e f a", "b b c e f a"),
			Row.of(3, "b d e f h", "d d e a c"),
			Row.of(4, "a c e d m", "a e e f b c")
		);
		BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, text1 string, text2 string");
		TextSimilarityPairwise similarity = new TextSimilarityPairwise().setSelectedCols("text1", "text2").setMetric(
			"LEVENSHTEIN").setOutputCol("output");
		similarity.transform(inOp).print();
	}
}