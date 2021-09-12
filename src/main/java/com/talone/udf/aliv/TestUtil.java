package com.talone.udf.aliv;

import java.io.File;

import com.aliyun.odps.local.common.WareHouse;

/**
 * Sample Java Class
 */
public class TestUtil {

    private final static String DEMO_PROJECT = "demo_project";

    /**
     * 数组拼接
     *
     * @param obj
     * @return
     */
    public static String join(Object[] obj) {
        if (obj == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < obj.length; i++) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(obj[i]);
        }
        return sb.toString();
    }

    /**
     * 初始化mock路径
     *
     * @return
     */
    public static WareHouse initWarehouse() {
        File exampleProjectDir = new File("warehouse" + File.separator + DEMO_PROJECT);
        if (exampleProjectDir.exists()) {
            return WareHouse.getInstance("warehouse");
        } else {
            exampleProjectDir = new File("../warehouse" + File.separator + DEMO_PROJECT);
            if (exampleProjectDir.exists()) {
                return WareHouse.getInstance("../warehouse");
            }
        }
        throw new RuntimeException("warehouse dir not exists");
    }

}
