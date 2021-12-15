package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import com.aliyun.odps.utils.StringUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Resolve({"string,string,string,string->string,string,string,string"})
public class TKQMinPriceUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        Double xprice = Double.parseDouble((String) args[0]);
        String activityInfo = (String) args[1];
        String tkqInfo = (String) args[2];
        String id = (String) args[3];

        if (xprice == 0D || StringUtils.isBlank(tkqInfo)) {
            forward(id, null, null, null);
        } else {
            Map map = tkfx(xprice, activityInfo, tkqInfo);
            forward(id, String.valueOf(map.get("xprice")), String.valueOf(map.get("num")), "0");
        }
    }

    private static Map tkfx(Double xprice, String activityInfo, String tkqInfo) {
        Map map = new HashMap();
        List<Double> tkq = new ArrayList<Double>();
        Double tke = 0D;
        Double tkj = 0D;
        tkqInfo = tkqInfo.replace(" ", "");
        String yhqzz = ".*满[0-9]+([.]{1}[0-9]+){0,1}+减[0-9]+([.]{1}[0-9]+){0,1}+.*";
        boolean yhqzzisMatch = Pattern.matches(yhqzz, tkqInfo);
        if (yhqzzisMatch) {
            for (String sss : tkqInfo.replaceAll("[^0-9.]", ",").split(",")) {
                if (sss.length() > 0)
                    tkq.add(Double.parseDouble(sss));
            }
        }

        if (tkq.size() % 2 == 0) {
            for (int i = 0; i < tkq.size(); i++) {
                if (i % 2 != 0) {
                    continue;
                }
                if (tkq.get(i) > tke) {
                    tke = tkq.get(i);
                    tkj = tkq.get(i + 1);
                }
            }
        }

        Integer buyNum = 0;
        Double totalPrice = 0d;

        if (null != tke) {
            while (totalPrice < tkj) {
                buyNum++;
                totalPrice = buyNum * xprice;
            }
        }

        String yhqzz2 = ".*满\\d+(\\D|)+减\\d+.*";
        String dzzz = ".*满[0-9]+([.]{1}[0-9]+){0,1}+件+[0-9]+([.]{1}[0-9]+){0,1}+折+.*";
        if (null != activityInfo && !"".equals(activityInfo)) {
            activityInfo = activityInfo.replaceAll("\\\\n", "|");
            String[] json = activityInfo.split("\"body\"");
            String body = "";
            if (json.length > 1) {
                body = json[1];
            }

            List<Integer> mj = new ArrayList<Integer>();
            List<String> zk = new ArrayList<String>();

            String[] yhqstrss = body.split("\\|");
            for (String yhqstr : yhqstrss) {
                // 满减
                yhqstr = yhqstr.replace(" ", "");
                boolean yhqzzisMatch1 = Pattern.matches(yhqzz2, yhqstr);
                if (yhqzzisMatch1) {
                    for (String sss : yhqstr.replaceAll("[^0-9]", ",").split(",")) {
                        if (sss.length() > 0)
                            mj.add(Integer.parseInt(sss));
                    }
                }

                // 打折
                boolean dzzzisMatch = Pattern.matches(dzzz, yhqstr);
                if (dzzzisMatch) {
                    yhqstr = yhqstr.substring(yhqstr.indexOf("满"));
                    for (String sss : yhqstr.replaceAll("[^0-9|\\.]", ",").split(",")) {
                        if (sss.length() > 0)
                            zk.add(sss);
                    }
                }
            }

            if(!CollectionUtils.isEmpty(mj)){
                Integer mje = 0;
                Integer mjj = 0;
                if (mj.size() % 2 == 0) {
                    for (int i = 0; i < mj.size(); i++) {
                        if (i % 2 != 0) {
                            continue;
                        }
                        if (mj.get(i) > mje && mj.get(i) <= totalPrice) {
                            mje = mj.get(i);
                            mjj = mj.get(i + 1);
                        }
                    }
                }
                tkj += mjj;
                totalPrice -= tkj;
            }else if(!CollectionUtils.isEmpty(zk)){
                Integer zkj = 0;
                Double zks = 0d;
                if (zk.size() % 2 == 0) {
                    for (int i = 0; i < zk.size(); i++) {
                        if (i % 2 != 0) {
                            continue;
                        }
                        if (Integer.parseInt(zk.get(i)) > zkj && Integer.parseInt(zk.get(i))<=buyNum) {
                            zkj = Integer.parseInt(zk.get(i));
                            zks = Double.parseDouble(zk.get(i + 1));
                        }
                    }
                }

                totalPrice = totalPrice*zks/10-tkj;

            }else{
                totalPrice -= tkj;
            }

        }else{
            totalPrice -= tkj;
        }

        if (totalPrice == 0d) {
            map.put("xprice", xprice);
            map.put("num", 1);
        } else {
            map.put("xprice", totalPrice / buyNum);
            map.put("num", buyNum);
        }
        return map;
    }


    public static void main(String[] args) {
        long s1 = System.currentTimeMillis();
        // 单价
        double xprice = 46;

        // 优惠券规则解析
        String yhqstrxx = "head: 雅塑白盒120粒 到2021-12-31 00:00:00结束, \"body\": 满90元 减1元 \\n 满260元 减45元 \\n 满360元 减110元 \\n 满550元 减170元 \\n 满1200元 减475元";

        //String yhqstrxx = "{\"head\": \"慕小腰日常满减 到2022-05-29 00:00:00结束\", \"body\": \" 满35.8元 减8元 \\n 满338元 减212元 \\n 满1014元 减642元 \\n 满1597元 减1012元 \"}";
        String tkqInfo = "满90减60";
        Map map = tkfx(xprice, yhqstrxx, tkqInfo);


        System.out.println(System.currentTimeMillis() - s1);

//        long s1 = System.currentTimeMillis();
//        // 单价
//        double xprice = 46;
//
//        // 优惠券规则解析
//        String yhqstrxx = "{\"head\": \"5.5折 到2021-12-11 23:59:59结束\", \"body\": \" 满1件 9.5折 \\n  满2件 8.5折 \"}";
//
//        //String yhqstrxx = "{\"head\": \"慕小腰日常满减 到2022-05-29 00:00:00结束\", \"body\": \" 满35.8元 减8元 \\n 满338元 减212元 \\n 满1014元 减642元 \\n 满1597元 减1012元 \"}";
//        String tkqInfo = "满90减60";
//        Map map = tkfx(xprice, yhqstrxx, tkqInfo);
//
//
//        System.out.println(System.currentTimeMillis() - s1);
    }
}
