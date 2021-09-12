package com.talone.udf.aliv.udtf;

import java.util.List;
import com.talone.udf.aliv.TestUtil;
import com.aliyun.odps.udf.local.datasource.InputSource;
import com.aliyun.odps.udf.local.datasource.TableInputSource;
import com.aliyun.odps.udf.local.runner.BaseRunner;
import com.aliyun.odps.udf.local.runner.UDTFRunner;
import org.junit.Assert;
import org.junit.Test;

/**
 * Sample Java Class
 */
public class StudioUDTFTest {

    public static void main(String[] args){
        TestUtil.initWarehouse();
        StudioUDTFTest studioUdafTest = new StudioUDTFTest() ;
        try {
			studioUdafTest.simpleInput();
            studioUdafTest.inputFromTable();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    @Test
    public void simpleInput() throws Exception {
        BaseRunner runner = new UDTFRunner(null, "com.alibaba.dataworks.udtf.StudioUDTF");
        runner.feed(new Object[] {"one", "one"}).feed(new Object[] {"three", "three"})
            .feed(new Object[] {"four", "four"});
        List<Object[]> out = runner.yield();
        Assert.assertEquals(3, out.size());
        Assert.assertEquals("one,3", TestUtil.join(out.get(0)));
        Assert.assertEquals("three,5", TestUtil.join(out.get(1)));
        Assert.assertEquals("four,4", TestUtil.join(out.get(2)));
    }

    @Test
    public void inputFromTable() throws Exception {
        BaseRunner runner = new UDTFRunner(null, "com.alibaba.dataworks.udtf.StudioUDTF");
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
        Assert.assertEquals(3, out.size());
        Assert.assertEquals("three3,6", TestUtil.join(out.get(1)));
    }

}
