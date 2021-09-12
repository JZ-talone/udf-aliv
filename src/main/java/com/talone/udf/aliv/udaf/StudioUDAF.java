package com.talone.udf.aliv.udaf;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.io.Text;

// TODO define input and output types, e.g. "double->double".
@Resolve({""})
public class StudioUDAF extends Aggregator {

    @Override
    public void iterate(Writable arg0, Writable[] arg1) {
        LongWritable result = (LongWritable)arg0;
        for (Writable item : arg1) {
            Text txt = (Text)item;
            result.set(result.get() + txt.getLength());
        }
    }

    @Override
    public void merge(Writable arg0, Writable arg1) {
        LongWritable result = (LongWritable)arg0;
        LongWritable partial = (LongWritable)arg1;
        result.set(result.get() + partial.get());

    }

    @Override
    public Writable newBuffer() {
        return new LongWritable(0L);
    }

    @Override
    public Writable terminate(Writable arg0) {
        return arg0;
    }

}
