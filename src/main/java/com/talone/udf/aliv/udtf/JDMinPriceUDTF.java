package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string,string,string->string,string,string,string"})
public class JDMinPriceUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        Double xprice = Double.parseDouble((String) args[0]);
        String yhqstrxx = (String) args[1];
        String cxstr = (String) args[2];
        String id = (String) args[3];

        if (xprice == 0D) {
            forward(id, "0", "0", "0");
        } else {
            // 优惠券规则解析
            Map yhqmap = gzfx(yhqstrxx);

            // 促销规则解析
            Map cxmap = gzfx(cxstr);

            Map map = mpfx(xprice, yhqmap, cxmap);

            boolean xgflag = null != cxstr && cxstr.contains("仅购买1件");
            forward(id, String.valueOf(map.get("xprice")), String.valueOf(map.get("num")), xgflag ? "1" : "0");
        }
    }

    public static void process2(Double xprice, String yhqstrxx, String cxstr) throws UDFException {

        if (xprice == 0D) {
            return;
        } else {
            // 优惠券规则解析
            Map yhqmap = gzfx(yhqstrxx);

            // 促销规则解析
            Map cxmap = gzfx(cxstr);

            Map map = mpfx(xprice, yhqmap, cxmap);

            boolean xgflag = null != cxstr && cxstr.contains("仅购买1件");
            System.out.println(String.valueOf(map.get("xprice")) + ":" + String.valueOf(map.get("num")));
        }
    }

    public static void main(String[] args) throws UDFException {
        process2(170.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "购买不超过200件时享受单件价￥170，超出数量以结算价为准|满299元减90元|满299元减90元|满160元减20元，满570元减40元");
        process2(170.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "购买不超过200件时享受单件价￥170，超出数量以结算价为准|满299元减90元|满299元减90元|满160元减20元，满570元减40元");
        process2(170.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "购买不超过200件时享受单件价￥170，超出数量以结算价为准|满299元减90元|满299元减90元|满160元减20元，满570元减40元");
        process2(192.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "满160元减20元，满570元减40元");
        process2(192.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券|以下商品可使用满79减20的优惠券", "满160元减20元，满570元减40元");
        process2(192.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "满299元减90元|满299元减90元|满160元减20元，满570元减40元");
        process2(192.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "满299元减90元|满299元减90元|满160元减20元，满570元减40元|购京东健康汽车用品农资园艺等部分自营商品满1元返券包");
        process2(192.0, "以下商品可使用满399减100的优惠券|(可叠加)以下商品可使用满200减30的优惠券|以下商品可使用满198减20的优惠券", "满299元减90元|满299元减90元|满160元减20元，满570元减40元");

//        // 单价
//        double xprice = 69.99;
//
//        // 优惠券规则解析
//        String yhqstrxx = "(可叠加)以下商品可使用满200减30的优惠券|(可叠加)以下商品可使用满99减10的优惠券|以下商品可使用满500减150的优惠券|以下商品可使用满1000减500的优惠券";
//        Map yhqmap = gzfx(yhqstrxx);
//
//        // 促销规则解析
//        String cxstr = "满2件，总价打8折；满3件，总价打7折|满680元减40元";
//        Map cxmap = gzfx(cxstr);
//
//        Map map = mpfx(xprice, yhqmap, cxmap);
//        System.out.println(1);
    }

    public static Map mpfx(double xprice, Map yhqmap, Map cxmap) {
        Map map = new HashMap();
        Integer cxmje = null;
        Integer cxmjj = null;
        Integer cxzkj = null;
        Double cxzks = null;
        Integer yhqmje = null;
        Integer yhqmjj = null;
        Integer yhqkdjmje = null;
        Integer yhqkdjmjj = null;
        if (null != cxmap.get("mje") && !cxmap.get("mje").equals(0)) {
            cxmje = Integer.parseInt(String.valueOf(cxmap.get("mje")));
            cxmjj = Integer.parseInt(String.valueOf(cxmap.get("mjj")));
        } else if (null != cxmap.get("zkj") && !cxmap.get("zkj").equals(0)) {
            cxzkj = Integer.parseInt(String.valueOf(cxmap.get("zkj")));
            cxzks = Double.parseDouble(String.valueOf(cxmap.get("zks")));
        }

        if (null != yhqmap.get("mje") && !yhqmap.get("mje").equals(0)) {
            yhqmje = Integer.parseInt(String.valueOf(yhqmap.get("mje")));
            yhqmjj = Integer.parseInt(String.valueOf(yhqmap.get("mjj")));
        }

        if (null != yhqmap.get("kdjmje") && !yhqmap.get("kdjmje").equals(0)) {
            yhqkdjmje = Integer.parseInt(String.valueOf(yhqmap.get("kdjmje")));
            yhqkdjmjj = Integer.parseInt(String.valueOf(yhqmap.get("kdjmjj")));
        }

        Integer buyNum = 0;
        Double totalPrice = 0d;
        // 促销
        // 暂时不知道促销是否同时存在折扣和满减两种方式 存在一种就排除另一种
        if (null != cxmje) {
            Double buyNumD = Double.parseDouble(String.valueOf(cxmje)) / xprice;
            buyNum = buyNumD.doubleValue() > buyNumD.intValue() ? buyNumD.intValue() + 1 : buyNumD.intValue();
            totalPrice = buyNum * xprice - cxmjj;
        }
        if (null != cxzkj) {
            buyNum = cxzkj;
            totalPrice = buyNum * xprice * cxzks / 10;
        }
        // 优惠券
        if (null != yhqmje) {
            if (totalPrice >= yhqmje) {
                totalPrice = totalPrice - yhqmjj;
            } else {
                while (totalPrice < yhqmje) {
                    buyNum++;
                    if (null == cxmje && null == cxzkj) {
                        totalPrice = buyNum * xprice;
                    } else if (null != cxmje) {
                        totalPrice = buyNum * xprice - cxmjj;
                    } else if (null != cxzkj) {
                        totalPrice = buyNum * xprice * cxzks / 10;
                    }
                }
                totalPrice = totalPrice - yhqmjj;
            }
        }
        // 可叠加优惠券
        if (null != yhqkdjmje) {
            if (totalPrice >= yhqkdjmje) {
                totalPrice = totalPrice - yhqkdjmjj;
            } else {
                while (totalPrice < yhqkdjmje) {
                    buyNum++;
                    if (null == cxmje && null == cxzkj) {
                        totalPrice = buyNum * xprice;
                    } else if (null != cxmje) {
                        totalPrice = buyNum * xprice - cxmjj;
                    } else if (null != cxzkj) {
                        totalPrice = buyNum * xprice * cxzks / 10;
                    }
                }
                totalPrice = totalPrice - (null != yhqmje ? yhqmjj : 0) - yhqkdjmjj;
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
        List<Integer> mj = new ArrayList<Integer>();
        List<Integer> kdjmj = new ArrayList<Integer>();
        List<String> zk = new ArrayList<String>();
        Integer mje = 0;
        Integer mjj = 0;

        Integer zkj = 0;
        Double zks = 0d;

        Integer kdjmje = 0;
        Integer kdjmjj = 0;

        String[] yhqstrss = yhqstrxx.split("\\|");
        for (String yhqstr : yhqstrss) {
            // 满减
            String yhqzz = ".*满\\d+(\\D|)+减\\d+.*";
            boolean yhqzzisMatch = Pattern.matches(yhqzz, yhqstr);
            if (yhqzzisMatch) {
//                yhqstr = yhqstr.substring(yhqstr.indexOf("满"));
                for (String sss : yhqstr.replaceAll("[^0-9]", ",").split(",")) {
                    if (sss.length() > 0)
                        if (yhqstr.contains("可叠加")) {
                            kdjmj.add(Integer.parseInt(sss));
                        } else {
                            mj.add(Integer.parseInt(sss));
                        }
                }
            }

            // 打折
            String dzzz = ".*满\\d+件(\\D|)+打\\d+.*";
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
        if (kdjmj.size() % 2 == 0) {
            for (int i = 0; i < kdjmj.size(); i++) {
                if (i % 2 != 0) {
                    continue;
                }
                if (kdjmj.get(i) > kdjmje) {
                    kdjmje = kdjmj.get(i);
                    kdjmjj = kdjmj.get(i + 1);
                }
            }
        }

        map.put("mje", mje);
        map.put("mjj", mjj);
        map.put("zkj", zkj);
        map.put("zks", zks);
        map.put("kdjmje", kdjmje);
        map.put("kdjmjj", kdjmjj);
        return map;
    }
}
