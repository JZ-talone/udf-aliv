package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Resolve({"string,string,string->string,string,string,string"})
public class TMMinPriceUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        Double xprice = Double.parseDouble((String) args[0]);
        String activityInfo = (String) args[1];
        String id = (String) args[2];

        if(xprice==0D){
            forward(id, "0", "0", "0");
        }else {
            // 活动规则解析
            Map yhqmap = gzfx(activityInfo);

            Map map = mpfx(xprice, yhqmap);
            forward(id, String.valueOf(map.get("xprice")), String.valueOf(map.get("num")), "0");
        }
    }

    public static void main(String[] args) {
//        long s1 = System.currentTimeMillis();
//        // 单价
//        double xprice = 99;
//
//        // 优惠券规则解析
//        String yhqstrxx = "{\"head\": \"雅塑主链接满减 到2021-12-09 00:00:00结束\", \"body\": \" 满50元 ,包邮 \\n 满520元 减80元 ,包邮 \\n 满650元 减141元 ,包邮 \\n 满800元 减180元 ,包邮 \\n 满2400元 减700元 ,包邮 \"}";
//        Map yhqmap = gzfx(yhqstrxx);
//
//        Map map = mpfx(xprice, yhqmap);
//        System.out.println(System.currentTimeMillis()-s1);


        long s1 = System.currentTimeMillis();
        // 单价
        double xprice = 715;

        // 优惠券规则解析
        String yhqstrxx = "{\"head\": \"5.5折 到2021-12-11 23:59:59结束\", \"body\": \" 满1件 5.5折 \\n  满3件 1.5折 \"}";
        Map yhqmap = gzfx(yhqstrxx);

        Map map = mpfx(xprice, yhqmap);
        System.out.println(System.currentTimeMillis()-s1);
    }

    public static Map mpfx(double xprice, Map yhqmap) {
        Map map = new HashMap();
        Integer cxzkj = null;
        Double cxzks = null;
        Integer yhqmje = null;
        Integer yhqmjj = null;

        if (null != yhqmap.get("mje") && !yhqmap.get("mje").equals(0)) {
            yhqmje = Integer.parseInt(String.valueOf(yhqmap.get("mje")));
            yhqmjj = Integer.parseInt(String.valueOf(yhqmap.get("mjj")));
        } else if (null != yhqmap.get("zkj") && !yhqmap.get("zkj").equals(0)) {
            cxzkj = Integer.parseInt(String.valueOf(yhqmap.get("zkj")));
            cxzks = Double.parseDouble(String.valueOf(yhqmap.get("zks")));
        }

        Integer buyNum = 0;
        Double totalPrice = 0d;
        // 优惠券
        if (null != yhqmje) {
            if (totalPrice >= yhqmje) {
                totalPrice = totalPrice - yhqmjj;
            } else {
                while (totalPrice < yhqmje) {
                    buyNum++;
                    totalPrice = buyNum * xprice;
                }
                totalPrice = totalPrice - yhqmjj;
            }
        } else if(null!=cxzkj){
            buyNum = cxzkj;
            totalPrice = buyNum * xprice * cxzks / 10;
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

    public static Map gzfx(String yhqstrxx) {
        Map map = new HashMap();
        if (null == yhqstrxx || "".equals(yhqstrxx)) {
            return map;
        }

        yhqstrxx = yhqstrxx.replaceAll("\\\\n","|");
        String[]  json = yhqstrxx.split("\"body\"");
        String body = "";
        if(json.length>1){
            body = json[1];
        }

        List<Integer> mj = new ArrayList<Integer>();
        List<String> zk = new ArrayList<String>();
        Integer mje = 0;
        Integer mjj = 0;

        Integer zkj = 0;
        Double zks = 0d;

        String[] yhqstrss = body.split("\\|");
        for (String yhqstr : yhqstrss) {
            // 满减
            yhqstr = yhqstr.replace(" ", "");
            String yhqzz = ".*满\\d+(\\D|)+减\\d+.*";
            boolean yhqzzisMatch = Pattern.matches(yhqzz, yhqstr);
            if (yhqzzisMatch) {
                for (String sss : yhqstr.replaceAll("[^0-9]", ",").split(",")) {
                    if (sss.length() > 0)
                        mj.add(Integer.parseInt(sss));
                }
            }


            // 打折
            String dzzz = ".*满[0-9]+([.]{1}[0-9]+){0,1}+件+[0-9]+([.]{1}[0-9]+){0,1}+折+.*";
            boolean dzzzisMatch = Pattern.matches(dzzz, yhqstr);
            if (dzzzisMatch) {
                yhqstr = yhqstr.substring(yhqstr.indexOf("满"));
                for (String sss : yhqstr.replaceAll("[^0-9|\\.]", ",").split(",")) {
                    if (sss.length() > 0)
                        zk.add(sss);
                }
            }
        }
        if (mj.size() % 2 == 0) {
            for (int i = 0; i < mj.size(); i++) {
                if (i % 2 != 0) {
                    continue;
                }
                if (mj.get(i) > mje) {
                    mje = mj.get(i);
                    mjj = mj.get(i + 1);
                }
            }
        }

        if (zk.size() % 2 == 0) {
            for (int i = 0; i < zk.size(); i++) {
                if (i % 2 != 0) {
                    continue;
                }
                if (Integer.parseInt(zk.get(i)) > zkj) {
                    zkj = Integer.parseInt(zk.get(i));
                    zks = Double.parseDouble(zk.get(i + 1));
                }
            }
        }

        map.put("mje", mje);
        map.put("mjj", mjj);
        map.put("zkj", zkj);
        map.put("zks", zks);
        return map;
    }
}
