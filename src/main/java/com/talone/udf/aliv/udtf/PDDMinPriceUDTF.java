package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Resolve({"string,string,string,string->string,string,string,string"})
public class PDDMinPriceUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        // 原价
        Double xprice = Double.parseDouble((String) args[0]);
        // 拼多多提示str
        String pddownqhprice = (String) args[1];
        // 折扣json
        String couponjson = (String) args[2];
        String id = (String) args[3];

        if (xprice == 0D) {
            forward(id, "0", "0", "0");
        } else {
            Double pddownprice = pddownfx(pddownqhprice);

            // 促销规则解析
            Map map = mpfx(xprice, pddownprice, couponjson);
            forward(id, String.valueOf(map.get("pddownprice")), String.valueOf(map.get("xprice")), String.valueOf(map.get("num")));
        }
    }

    private static Map mpfx(Double xprice, Double pddownprice, String couponjson) {
        Map map = new HashMap();
        try {
            if (StringUtils.isBlank(couponjson)) {
                map.put("xprice", null != pddownprice ? pddownprice : xprice);
                map.put("num", 1);
                map.put("pddownprice", pddownprice);
                return map;
            } else {
                if (couponjson.contains("mallCouponBatchTitle") && null != pddownprice) {
                    // 包含无门槛券直接返回券后价
                    map.put("xprice", pddownprice);
                    map.put("num", 1);
                    map.put("pddownprice", pddownprice);
                    return map;
                } else {
                    String[] xx = couponjson.split("'满\\d+(\\D|)+减\\d+元'");
                    for (String s : xx) {
                        couponjson = couponjson.replace(s, ",");
                    }
                    String[] eventDescs = couponjson.split(",");
                    List<Integer> mj = new ArrayList<Integer>();
                    for (String eventDesc : eventDescs) {
                        for (String sss : eventDesc.replaceAll("[^0-9]", ",").split(",")) {
                            if (sss.length() > 0) {
                                mj.add(Integer.parseInt(sss));
                            }
                        }
                    }
                    if (null != mj && mj.size() > 0) {
                        Integer mje = 0;
                        Integer mjj = 0;
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
                        Double x = mje / xprice;
                        Integer num = x.intValue() + 1;
                        xprice = (xprice * num - mjj) / num;
                        map.put("xprice", xprice);
                        map.put("num", num);
                        map.put("pddownprice", pddownprice);
                        return map;
                    } else {
                        map.put("xprice", null != pddownprice ? pddownprice : xprice);
                        map.put("num", 1);
                        map.put("pddownprice", pddownprice);
                        return map;
                    }
                }
            }
        } catch (Exception e) {
            map.put("xprice", null != pddownprice ? pddownprice : xprice);
            map.put("num", 1);
            map.put("pddownprice", pddownprice);
            return map;
        }

    }

    private static Double pddownfx(String pddownqhprice) {
        if (StringUtils.isBlank(pddownqhprice)) {
            return null;
        } else {
            if (pddownqhprice.contains("券后")) {
                List<Double> pddwone = new ArrayList<>();
                String xx = pddownqhprice.split("券后")[1];
                for (String sss : xx.replaceAll("[^0123456789.]", ",").split(",")) {
                    if (sss.length() > 0)
                        pddwone.add(Double.parseDouble(sss));
                }
                return pddwone.get(0);
            } else {
                return null;
            }
        }
    }


    public static void main(String[] args) throws UDFException {
        // 原价
        Double xprice = 19.99;
        // 拼多多提示str
        String pddownqhprice = "券后￥9.99起";
        // 折扣json
        String couponjson = "{'title': '优惠详情', 'mallPromotionEventDetail': {'mallEventTitle': '店铺活动', 'promoEventSn': 'Z0659MM-588001226659874456', 'mallId': 255952536, 'mallEventDesc': [{'txt': '入选店内'}, {'txt': '满299元减20元', 'color': '#E02E24'}, {'txt': '专区'}], 'buttonDesc': '去看看', 'jumpUrl': 'likes.html?_t_timestamp=likes_merge_list&mall_id=255952536&promotion_event_sn=Z0659MM-588001226659874456&top_goods_ids=203938528049&select_goods_ids=203938528049&last_cart=[203938528049]', 'isCouponStyle': False}, 'mallPromotionEventTitle': '店铺活动', 'mallPromotionEventDetailList': [{'mallEventTitle': '店铺活动', 'promoEventSn': 'Z0659MM-588001226659874456', 'mallId': 255952536, 'mallEventDesc': [{'txt': '入选店内'}, {'txt': '满299元减20元', 'color': '#E02E24'}, {'txt': '专区'}], 'buttonDesc': '去看看', 'jumpUrl': 'likes.html?_t_timestamp=likes_merge_list&mall_id=255952536&promotion_event_sn=Z0659MM-588001226659874456&top_goods_ids=203938528049&select_goods_ids=203938528049&last_cart=[203938528049]', 'isCouponStyle': True, 'tag': '满减专区', 'discountParam': 2000, 'minOrderAmount': 29900, 'discountDesc': '满299元减20元', 'discountTagDesc': '活动专区商品可用', 'discountType': 1}], 'giftPromotionDetails': {'title': '赠品', 'rulesDesc': '部分规格拼单可得以下赠品（赠完即止）', 'giftGoodsName': '碧生源维生素C泡腾片甜橙味固体饮料儿童成人维他命VC维C泡腾片', 'thumbnailUrl': 'https://img.pddpic.com/mms-material-img/2021-10-11/ed945a12-1580-4d83-9f6f-6a194c05a2f2.jpeg.a.jpeg'}, 'environmentContext': {'pageFrom': '0', 'newVersion': True, 'functionTag': False}, 'isUnavailable': False}";
        String id = "11";

//        // 原价
//        Double xprice = 269.99;
//        // 拼多多提示str
//        String pddownqhprice = "";
//        // 折扣json
//        String couponjson = "{\"title\": \"优惠详情\", \"mallPromotionEventDetail\": {\"mallEventTitle\": \"店铺活动\", \"promoEventSn\": \"Z0659MM-588001226659874456\", \"mallId\": 255952536, \"mallEventDesc\": [{\"txt\": \"入选店内\"}, {\"txt\": \"满299元减20元\", \"color\": \"#E02E24\"}, {\"txt\": \"专区\"}], \"buttonDesc\": \"去看看\", \"jumpUrl\": \"likes.html?_t_timestamp=likes_merge_list&mall_id=255952536&promotion_event_sn=Z0659MM-588001226659874456&top_goods_ids=203938528049&select_goods_ids=203938528049&last_cart=[203938528049]\", \"isCouponStyle\": \"False\"}, \"mallPromotionEventTitle\": \"店铺活动\", \"mallPromotionEventDetailList\": [{\"mallEventTitle\": \"店铺活动\", \"promoEventSn\": \"Z0659MM-588001226659874456\", \"mallId\": 255952536, \"mallEventDesc\": [{\"txt\": \"入选店内\"}, {\"txt\": \"满299元减20元\", \"color\": \"#E02E24\"}, {\"txt\": \"专区\"}], \"buttonDesc\": \"去看看\", \"jumpUrl\": \"likes.html?_t_timestamp=likes_merge_list&mall_id=255952536&promotion_event_sn=Z0659MM-588001226659874456&top_goods_ids=203938528049&select_goods_ids=203938528049&last_cart=[203938528049]\", \"isCouponStyle\": \"True\", \"tag\": \"满减专区\", \"discountParam\": 2000, \"minOrderAmount\": 29900, \"discountDesc\": \"满299元减20元\", \"discountTagDesc\": \"活动专区商品可用\", \"discountType\": 1}], \"giftPromotionDetails\": {\"title\": \"赠品\", \"rulesDesc\": \"部分规格拼单可得以下赠品（赠完即止）\", \"giftGoodsName\": \"碧生源维生素C泡腾片甜橙味固体饮料儿童成人维他命VC维C泡腾片\", \"thumbnailUrl\": \"https://img.pddpic.com/mms-material-img/2021-10-11/ed945a12-1580-4d83-9f6f-6a194c05a2f2.jpeg.a.jpeg\"}, \"environmentContext\": {\"pageFrom\": \"0\", \"newVersion\": \"True\", \"functionTag\": \"False\"}, \"isUnavailable\": \"False\"}";
//        String id = "11";

        Double pddownprice = pddownfx(pddownqhprice);

        // 促销规则解析
        Map map = mpfx(xprice, pddownprice, couponjson);
        System.out.println(1);
    }


}
