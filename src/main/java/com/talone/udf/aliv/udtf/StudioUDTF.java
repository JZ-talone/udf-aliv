package com.talone.udf.aliv.udtf;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string->string,bigint"})
public class StudioUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        String a = (String)args[0];
        long b = args[1] == null ? 0 : ((String)args[1]).length();
        forward(a, b);
    }
}
