package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

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

        if (xprice == 0D) {
            forward(id, "0", "0", "0");
        } else {
            Map map = tkfx(xprice, activityInfo, tkqInfo);
            forward(id, String.valueOf(map.get("xprice")), String.valueOf(map.get("num")), "0");
        }
    }

    private static Map tkfx(Double xprice, String activityInfo, String tkqInfo) {
        Map map = new HashMap();
        List<Integer> tkq = new ArrayList<Integer>();
        Integer tke = 0;
        Integer tkj = 0;
        tkqInfo = tkqInfo.replace(" ", "");
        String yhqzz = ".*满\\d+(\\D|)+减\\d+.*";
        boolean yhqzzisMatch = Pattern.matches(yhqzz, tkqInfo);
        if (yhqzzisMatch) {
            for (String sss : tkqInfo.replaceAll("[^0-9]", ",").split(",")) {
                if (sss.length() > 0)
                    tkq.add(Integer.parseInt(sss));
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
            if (totalPrice < tkj) {
                buyNum++;
                totalPrice = buyNum * xprice;
            }
        }

        if (null != activityInfo && !"".equals(activityInfo)) {
            activityInfo = activityInfo.replaceAll("\n", "|");
            String[] json = activityInfo.split("\"body\"");
            String body = "";
            if (json.length > 1) {
                body = json[1];
            }

            List<Integer> mj = new ArrayList<Integer>();

            String[] yhqstrss = body.split("\n");
            for (String yhqstr : yhqstrss) {
                // 满减
                yhqstr = yhqstr.replace(" ", "");
                boolean yhqzzisMatch1 = Pattern.matches(yhqzz, yhqstr);
                if (yhqzzisMatch1) {
                    for (String sss : yhqstr.replaceAll("[^0-9]", ",").split(",")) {
                        if (sss.length() > 0)
                            mj.add(Integer.parseInt(sss));
                    }
                }
            }

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
        double xprice = 77;

        // 优惠券规则解析
        String yhqstrxx = "{\"head\": \"雅塑72粒 到2021-10-31 00:00:00结束\", \"body\": \" 满77元 减15元 \\n 满790元 减120元 \\n 满1700元 减400元 \"}";
        String tkqInfo = "满74减25";
        Map map = tkfx(xprice, yhqstrxx, tkqInfo);


        System.out.println(System.currentTimeMillis() - s1);
    }
}
