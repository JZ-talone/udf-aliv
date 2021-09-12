package com.talone.udf.aliv.udaf;

import com.aliyun.odps.udf.local.runner.BaseRunner;

import com.talone.udf.aliv.TestUtil;
import org.junit.Assert;

import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.runner.AggregatorRunner;

import java.util.List;

/**
 * Sample Java Class
 */
public class StudioUDAFTest {

    public static void main(String[] args) throws Exception {
        // 测初始化
        TestUtil.initWarehouse();
        simpleInput() ;
        inputFromTable();
    }

    public static void simpleInput() throws Exception {
        BaseRunner runner = new AggregatorRunner(null,
            "com.alibaba.dataworks.udaf.StudioUDAF");
        runner.feed(new Object[] {"one", "one"}).feed(new Object[] {"three", "three"})
            .feed(new Object[] {"four", "four"});
        List<Object[]> out = runner.yield();
        Assert.assertEquals(1, out.size());
        System.out.println("size:" + out.size());
        Assert.assertEquals(24L, out.get(0)[0]);
        System.out.println("get:" + out.get(0)[0]);
    }

    public static void inputFromTable() throws Exception {
        BaseRunner runner = new AggregatorRunner(null,
            "com.alibaba.dataworks.udaf.StudioUDAF");
        String project = "studio_project";
        String table = "wc_in2";
        String[] partitions = new String[] {"p2=1", "p1=2"};
        String[] columns = new String[] {"colc", "cola"};
        InputSource inputSource = new TableInputSource(project, table, partitions, columns);
        Object[] data;
        while ((data = inputSource.getNextRow()) != null) {
            runner.feed(data);
        }
        List<Object[]> out = runner.yield();
        Assert.assertEquals(1, out.size());
        Assert.assertEquals(100L, out.get(0)[0]);
        System.out.println("get:" + out.get(0)[0]);
    }
}