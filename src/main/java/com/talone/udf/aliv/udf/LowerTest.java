package com.talone.udf.aliv.udf;

import com.talone.udf.aliv.TestUtil;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.runner.BaseRunner;
import com.aliyun.odps.udf.local.runner.UDFRunner;
import com.aliyun.odps.udf.local.datasource.TableInputSource;

import java.util.List;

/**
 * Sample Java Class
 */
public class LowerTest {
    public static void main(String[] args) throws Exception {
        TestUtil.initWarehouse();

        BaseRunner runner = new UDFRunner(null, "com.alibaba.dataworks.udf.Lower");

        TableInfo tableInfo = TableInfo.builder().projectName("demo_project").tableName("demo_01").cols(
            new String[] {"name"})
            .build();
        InputSource inputSource = new TableInputSource(tableInfo);

        Object[] data;
        while ((data = inputSource.getNextRow()) != null) {
            runner.feed(data);
        }
        List<Object[]> out = runner.yield();
        System.out.println(TestUtil.join(out.get(0)));
        System.out.println(TestUtil.join(out.get(1)));
        System.out.println(TestUtil.join(out.get(2)));
        System.out.println(TestUtil.join(out.get(3)));
    }
}
