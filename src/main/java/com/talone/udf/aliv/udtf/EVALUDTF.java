package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string->string,bigint"})
public class EVALUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        String evalpress = (String) args[0];

        if (null == evalpress || evalpress.length() == 0) {
            forward(evalpress, 1L);
        } else {
            try {
                String x = evalpress.replaceAll("[^0-9|*]", "").replaceAll("\\|", "");
                AtomicReference<Long> ans = new AtomicReference<>(1L);
                String[] evalparam = x.split("\\*");
                Arrays.stream(evalparam).forEach((param) -> {
                    if (null != param && param.length() > 0) {
                        ans.updateAndGet(v -> v * Long.parseLong(param));
                    }
                });
                forward(evalpress, ans.get());
            }catch (Exception e) {
                forward(evalpress, 1L);
            }
        }
    }

    public static void main(String[] args) {
        String evalpress = "5粒|*5板";
        String x = evalpress.replaceAll("[^0-9|*]", "").replaceAll("\\|", "");
        AtomicReference<Long> ans = new AtomicReference<>(1L);
        String[] evalparam = x.split("\\*");
        Arrays.stream(evalparam).forEach((param) -> {
            if (null != param && param.length() > 0) {
                ans.updateAndGet(v -> v * Long.parseLong(param));
            }
        });
        System.out.println(ans.get());
    }

}
