package com.sniff;

import com.alibaba.alink.operator.batch.recommendation.BaseRecommBatchOp;
import org.apache.flink.types.Row;

public class row_test {

    public static void main(String[] args) {

        // 封装的快捷构建方法
        Row u6 = Row.of("u6", "0.0 1.0", 0.0, 1.0, 1, "{\"data\":{\"iid\":[18,19,88]},"
                + "\"schema\":\"iid INT\"}");

        System.out.println(u6.getArity());
        System.out.println(u6.getField(5));

        u6.setField(0,"u5");

        System.out.println(u6.getKind());

        System.out.println(u6.getField(0));

        Row join = Row.join(u6, u6);
        System.out.println(join.getKind());
        System.out.println(join.getArity());
        System.out.println(join);
        BaseRecommBatchOp

    }
}
