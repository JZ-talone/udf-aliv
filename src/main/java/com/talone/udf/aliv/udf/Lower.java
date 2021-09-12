package com.talone.udf.aliv.udf;

import com.aliyun.odps.udf.UDF;

public final class Lower extends UDF {
    public String evaluate(String s) {
        if (s == null) { return null ;}
        return s.toLowerCase();
    }

    public String foo2(String str){
        if(str == null){
            return null;
        }

        return str.toLowerCase();
    }
}