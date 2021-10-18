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
        long s1 = System.currentTimeMillis();
        // 单价
        double xprice = 119;

        // 优惠券规则解析
        String yhqstrxx = "{\"head\": \"雅塑72粒 到2021-10-31 00:00:00结束\", \"body\": \" 满400元 减60元 \\n 满790元 减120元 \\n 满1700元 减400元 \"}";
        Map yhqmap = gzfx(yhqstrxx);

        Map map = mpfx(xprice, yhqmap);
        System.out.println(System.currentTimeMillis()-s1);
    }

    public static Map mpfx(double xprice, Map yhqmap) {
        Map map = new HashMap();
        Integer yhqmje = null;
        Integer yhqmjj = null;

        if (null != yhqmap.get("mje") && !yhqmap.get("mje").equals(0)) {
            yhqmje = Integer.parseInt(String.valueOf(yhqmap.get("mje")));
            yhqmjj = Integer.parseInt(String.valueOf(yhqmap.get("mjj")));
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

        yhqstrxx = yhqstrxx.replaceAll("\n","|");
        String[]  json = yhqstrxx.split("\"body\"");
        String body = "";
        if(json.length>1){
            body = json[1];
        }

        List<Integer> mj = new ArrayList<Integer>();
        Integer mje = 0;
        Integer mjj = 0;

        String[] yhqstrss = body.split("\n");
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

        map.put("mje", mje);
        map.put("mjj", mjj);
        return map;
    }
}
