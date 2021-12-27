package com.talone.udf.aliv.udtf;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Resolve({"string,string,string,string->string,string,string,string,bigint,bigint,bigint"})
public class SkuDrugUnitUDTF extends UDTF {

    @Override
    public void process(Object[] args) throws UDFException {
        String id = (String) args[0];
        String masterPackage = (String) args[1];
        String spuTitle = (String) args[2];
        String skuTitle = (String) args[3];
        Long masterDrugUnit = masterDrugUnitFx(masterPackage);
        Map<String, Long> titleParse = titleParseFx(spuTitle, skuTitle);
        Long totalLs = null, totalHs = null;
        if (null != titleParse.get("ls")) {
            totalLs = titleParse.get("ls");
            totalHs = totalLs / masterDrugUnit;
        } else if (null != titleParse.get("hs")) {
            totalHs = titleParse.get("hs");
            totalLs = totalHs * masterDrugUnit;
        }

        if (masterDrugUnit == 0D) {
            forward(id, masterPackage, spuTitle, skuTitle, 0L, 0L, 0L);
        } else {
            forward(id, masterPackage, spuTitle, skuTitle, masterDrugUnit, totalLs, totalHs);
        }
    }

    public static List<String> getMatchers(String regex, String source) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(source);
        List<String> list = new ArrayList<>();
        while (matcher.find()) {
            list.add(matcher.group());
        }
        return list;
    }

    private static Long masterDrugUnitFx(String masterPackage) {
        if (null == masterPackage || "".equals(masterPackage)) {
            return 0L;
        }
        // 去除可能的 (*) 及 +*
        int x = masterPackage.indexOf("(");
        int y = masterPackage.indexOf("+");
        x = (x == -1 ? Integer.MAX_VALUE : x) < (y == -1 ? Integer.MAX_VALUE : y) ? x : y;
        if (x > -1) {
            masterPackage = masterPackage.substring(0, x);
        }
        List<String> matchers = getMatchers("([0-9]+([.]{1}[0-9]+){0,1}(粒|片|板|盒|瓶|小盒|袋))", masterPackage);

        Long ans = 1L;
        for (String matcher : matchers) {
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            Matcher mat = pattern.matcher(matcher);
            if (mat.find()) {
                ans *= Long.parseLong(mat.group());
            }
        }

        return ans;
    }

    private static Map<String, Long> titleParseFx(String spuTitle, String skuTitle) {
        // todo 处理 加送xxx x粒 的情况
        Pattern pn = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");

        Map<String, Long> map = new HashMap<>();
        // 解析skuTitle
        // case skutitle包含 共x粒;
        Pattern p1 = Pattern.compile("(共|直发)[0-9]+([.]{1}[0-9]+){0,1}粒");
        Matcher m1 = p1.matcher(skuTitle);
        if (m1.find()) {
            String m1g = m1.group();
            Matcher mn = pn.matcher(m1g);
            mn.find();
            Long zl = Long.parseLong(mn.group());
            map.put("ls", zl);
            return map;
        }
        // case 24+24粒 24粒+24粒 24粒多得24粒
        skuTitle = skuTitle.replaceAll("\\+下单多得|\\+多得|多得", "+");
        // 空格 一二三四 粒装 盒装 的 X
        skuTitle = skuTitle.replaceAll(" ", "")
                .replaceAll("一", "1")
                .replaceAll("一", "1")
                .replaceAll("二", "2")
                .replaceAll("三", "3")
                .replaceAll("四", "4")
                .replaceAll("五", "5")
                .replaceAll("六", "6")
                .replaceAll("七", "7")
                .replaceAll("八", "8")
                .replaceAll("九", "9")
                .replaceAll("十", "10")
                .replaceAll("粒装", "粒")
                .replaceAll("盒装", "盒")
                .replaceAll("的", "*")
                .replaceAll("x", "*")
                .replaceAll("X", "*");
        /**
         * 分类讨论
         *
         * 盒数粒数分别有三种情况 0个 1个 n个
         * 组合九种大类
         */
        boolean h1 = Pattern.matches(".*[0-9]+([.]{1}[0-9]+){0,1}盒.*", skuTitle);
        boolean hn = Pattern.matches(".*[0-9]+([.]{1}[0-9]+){0,1}盒.*[0-9]+([.]{1}[0-9]+){0,1}盒.*", skuTitle);
        boolean l1 = Pattern.matches(".*[0-9]+([.]{1}[0-9]+){0,1}粒.*", skuTitle);
        boolean ln = Pattern.matches(".*[0-9]+([.]{1}[0-9]+){0,1}粒.*[0-9]+([.]{1}[0-9]+){0,1}粒.*", skuTitle);

        if (!h1 && !l1) {
            // 0盒数0粒数     得1盒
            Long hs = 1L;
            // 获取spu的单盒粒数
            Long spuLs = spuLsParse(spuTitle);
            if (null != spuLs && 0 != spuLs) {
                map.put("ls", hs * spuLs);
            } else {
                map.put("hs", hs);
            }
            return map;
        }

        if (h1 && !hn && !l1 && !ln) {
            // 1盒数0粒数    得正常盒数
            List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}盒", skuTitle);

            Long hs = 1L;
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            Matcher mat = pattern.matcher(matchers.get(0));
            if (mat.find()) {
                hs = Long.parseLong(mat.group());
            }
            // 获取spu的单盒粒数
            Long spuLs = spuLsParse(spuTitle);
            if (null != spuLs && 0 != spuLs) {
                map.put("ls", hs * spuLs);
            } else {
                map.put("hs", hs);
            }
            return map;
        }

        if (!h1 && !hn && l1 && !ln) {
            // 1粒数0盒数
            List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}粒", skuTitle);

            Long ls = 1L;
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            Matcher mat = pattern.matcher(matchers.get(0));
            if (mat.find()) {
                ls = Long.parseLong(mat.group());
            }
            map.put("ls", ls);
            return map;
        }

        if (!h1 && !hn && ln) {
            // n粒数0盒数 24粒+24粒     买128粒 送xxx12粒
            boolean jl = Pattern.matches(".*[0-9]+([.]{1}[0-9]+){0,1}粒\\+[0-9]+([.]{1}[0-9]+){0,1}粒.*", skuTitle);
            if (jl) {
                List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}粒\\+[0-9]+([.]{1}[0-9]+){0,1}粒", skuTitle);
                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                for (String matcher : matchers) {
                    Matcher mat = pattern.matcher(matcher);
                    Long curLs = 0L;
                    while (mat.find()) {
                        curLs += Long.parseLong(mat.group());
                    }
                    ls = ls > curLs ? ls : curLs;
                }
                map.put("ls", ls);
            } else {
                List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}粒", skuTitle);

                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                for (String matcher : matchers) {
                    Matcher mat = pattern.matcher(matcher);
                    if (mat.find()) {
                        ls = ls > Long.parseLong(mat.group()) ? ls : Long.parseLong(mat.group());
                        map.put("ls", ls);
                    }
                }
            }
            return map;
        }

        if (hn && !l1 && !ln) {
            // 0粒数n盒数     得较大盒数
            List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}盒", skuTitle);

            Long hs = 1L;
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            for (String matcher : matchers) {
                Matcher mat = pattern.matcher(matcher);
                if (mat.find()) {
                    hs = hs > Long.parseLong(mat.group()) ? hs : Long.parseLong(mat.group());
                }
            }

            // 获取spu的单盒粒数
            Long spuLs = spuLsParse(spuTitle);
            if (null != spuLs && 0 != spuLs) {
                map.put("ls", hs * spuLs);
            } else {
                map.put("hs", hs);
            }
            return map;
        }

        if (!hn && h1 && !ln && l1) {
            // 1粒数1盒数     有乘号相乘  无乘号判断
            boolean chtrue = Pattern.matches(".*(([0-9]+([.]{1}[0-9]+){0,1}粒\\*[0-9]+([.]{1}[0-9]+){0,1}盒)|([0-9]+([.]{1}[0-9]+){0,1}盒\\*[0-9]+([.]{1}[0-9]+){0,1}粒)).*", skuTitle);
            if (chtrue) {
                List<String> matchers = getMatchers("(([0-9]+([.]{1}[0-9]+){0,1}粒\\*[0-9]+([.]{1}[0-9]+){0,1}盒)|([0-9]+([.]{1}[0-9]+){0,1}盒\\*[0-9]+([.]{1}[0-9]+){0,1}粒))", skuTitle);
                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                Matcher mat = pattern.matcher(matchers.get(0));
                while (mat.find()) {
                    ls *= Long.parseLong(mat.group());
                }
                map.put("ls", ls);
            } else {
                List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}(粒|盒)", skuTitle);
                Long hs = 1L;
                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                for (String matcher : matchers) {
                    Matcher mat = pattern.matcher(matcher);
                    if (mat.find()) {
                        if (matcher.contains("盒")) {
                            hs = Long.parseLong(mat.group());
                        }
                        if (matcher.contains("粒")) {
                            ls = Long.parseLong(mat.group());
                        }
                    }
                }
                ls = totalLsFunk(ls, hs);
                map.put("ls", ls);
            }
            return map;
        }

        if (!hn && h1 && ln) {
            // n粒数1盒数 取最大粒数即可覆盖大部分case
            List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}粒", skuTitle);

            Long ls = 1L;
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            for (String matcher : matchers) {
                Matcher mat = pattern.matcher(matcher);
                if (mat.find()) {
                    ls = ls > Long.parseLong(mat.group()) ? ls : Long.parseLong(mat.group());
                }
            }
            map.put("ls", ls);
            return map;
        }

        if (!ln && l1 && hn) {
            // 1粒数n盒数  先看是否有乘数 有乘数直接相乘   无则根据粒数index 就近原则取盒数  相同时左优先
            boolean chtrue = Pattern.matches(".*(([0-9]+([.]{1}[0-9]+){0,1}粒\\*[0-9]+([.]{1}[0-9]+){0,1}盒)|([0-9]+([.]{1}[0-9]+){0,1}盒\\*[0-9]+([.]{1}[0-9]+){0,1}粒)).*", skuTitle);
            if (chtrue) {
                List<String> matchers = getMatchers("(([0-9]+([.]{1}[0-9]+){0,1}粒\\*[0-9]+([.]{1}[0-9]+){0,1}盒)|([0-9]+([.]{1}[0-9]+){0,1}盒\\*[0-9]+([.]{1}[0-9]+){0,1}粒))", skuTitle);
                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                Matcher mat = pattern.matcher(matchers.get(0));
                while (mat.find()) {
                    ls *= Long.parseLong(mat.group());
                }
                map.put("ls", ls);
            } else {
                List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}(粒|盒)", skuTitle);
                Integer lindex = -1;
                List<Integer> hindex = new ArrayList<>();
                String lmatcher = null;
                List<String> hmatchers = new ArrayList<>();
                for (String matcher : matchers) {
                    Integer index = skuTitle.indexOf(matcher);
                    if (matcher.contains("粒")) {
                        lindex = index;
                        lmatcher = matcher;
                    } else {
                        hindex.add(index);
                        hmatchers.add(matcher);
                    }
                }
                Integer index = 0;
                Integer minDistance = Integer.MAX_VALUE;
                for (int i = 0; i < hindex.size(); i++) {
                    Integer curHindex = hindex.get(i);
                    if (Math.abs(curHindex - lindex) < minDistance) {
                        minDistance = Math.abs(curHindex - lindex);
                        index = i;
                    }
                }

                Long hs = 1L;
                Long ls = 1L;
                Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                Matcher mat = pattern.matcher(lmatcher);
                if (mat.find()) {
                    ls = Long.parseLong(mat.group());
                }

                Pattern pattern2 = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
                Matcher mat2 = pattern2.matcher(hmatchers.get(index));
                if (mat2.find()) {
                    hs = Long.parseLong(mat2.group());
                }

                ls = totalLsFunk(ls, hs);
                map.put("ls", ls);
            }
            return map;
        }

        if (ln && hn) {
            // 多盒数多粒数 拆分子句分類計算
            List<String> zjs = new ArrayList<>();
            boolean zjtrue = Pattern.matches(".*(((\\(|（).*(\\)|）))|(【.*】)).*", skuTitle);
            if (zjtrue) {
                List<String> matchers = getMatchers("(((\\(|（).*(\\)|）))|(【.*】))", skuTitle);
                String skuTitle2 = skuTitle.replaceAll("(((\\(|（).*(\\)|）))|(【.*】))", "");
                zjs.addAll(matchers);
                zjs.add(skuTitle2);
            } else {
                zjs.add(skuTitle);
            }
            Long ls = 0L;
            for (String zj : zjs) {
                Long zjparse = zjParseFunc(zj);
                ls = zjparse > ls ? zjparse : ls;
            }
            map.put("ls", ls);
            return map;
        }

        return map;
    }

    private static Long spuLsParse(String spuTitle) {
        List<String> matchers = getMatchers("[0-9]+([.]{1}[0-9]+){0,1}粒", spuTitle);
        Long ls = 0L;
        for (String matcher : matchers) {
            Pattern pattern = Pattern.compile("[0-9]+([.]{1}[0-9]+){0,1}");
            Matcher mat = pattern.matcher(matcher);
            while (mat.find()) {
                ls = Math.max(Long.parseLong(mat.group()), ls);
            }
        }
        return ls;
    }

    private static Long zjParseFunc(String zj) {
        Long ls = 0L;
        for (String s : zj.split("\\+")) {
            Map<String, Long> addParse = titleParseFx("", s);
            ls += (null == addParse.get("ls") ? 0L : addParse.get("ls"));
        }
        return ls;
    }

    private static Long totalLsFunk(Long ls, Long hs) {
        if (ls % hs != 0) {
            // 粒数无法被整除 极大概率为单盒粒数
            ls *= hs;
        } else if (ls > 21) {
            // 单盒 粒数>21 的极少
        } else if (ls / hs < 3) {
            // 单盒粒数<3的极少
            ls *= hs;
        } else {
            // todo 需要验证更多case
            ls *= hs;
        }
        return ls;
    }


    public static void main(String[] args) throws UDFException {
//        String spuTitle = "碧生源奥利司他胶囊48粒抗肥胖排油减脂瘦身减重减肥药产品正品";
//        String skuTitle = "19粒";
//        Map<String, Long> titleParse = titleParseFx(spuTitle, skuTitle);
//        System.out.println("zl:" + titleParse.get("zl") + ",zh:" + titleParse.get("zh"));

//        String masterPackage = "60mg*12粒*4板";
//        Long masterDrugUnit = masterDrugUnitFx(masterPackage);
//        System.out.println(masterDrugUnit);


//        String spuTitle = "碧生源奥利司他胶囊48粒抗肥胖排油减脂瘦身减重减肥药产品正品";
//        String skuTitle = "3盒30粒+8粒（共98粒）";
//        Map<String, Long> titleParse = titleParseFx(spuTitle, skuTitle);
//        System.out.println("zl:" + titleParse.get("zl") + ",zh:" + titleParse.get("zh"));


//        String spuTitle = "碧生源奥利司他胶囊48粒抗肥胖排油减脂瘦身减重减肥药产品正品";
//        String skuTitle = "19粒";
//        Map<String, Long> titleParse = titleParseFx(spuTitle, skuTitle);
//        System.out.println("zl:" + titleParse.get("zl") + ",zh:" + titleParse.get("zh"));

//        List<String> list = new ArrayList<>();
//        list.add("碧奥8粒+碧生源维生素C60粒");
//        list.add("3盒30粒+来利奥利司他6粒");
//        list.add("【6粒仅体验】建议购买24+24粒");
//        list.add("实发 18粒-5 盒");
//        list.add("1盒");
//        list.add("标准装");
//        list.add("7粒装三盒");
//        list.add("0.12g*18粒的一盒");
//        list.add("0.12g*18粒X3盒");
//        list.add("7粒装-四盒装");
//        list.add("3盒（0.12g*3粒/盒）优惠");
//        list.add("超值装90组合装");
//        list.add("【12粒装】2盒装");
//        list.add("0.12gX90粒(18粒/5盒)");
//        list.add("90粒黑盒(18粒2盒+3粒18盒）");
//        list.add("一盒（体验 3粒/盒）");
//        list.add("一盒（标准 18粒/盒）推荐");
//        list.add("0.12gX90粒(18粒/5盒)");
//        list.add("0.12gX180粒(18粒10盒)");
//        list.add("90粒黑盒(18粒2盒+3粒18盒）");
//        list.add("黑盒90粒(3粒/30盒)");
//        list.add("黑盒30粒(3粒/10盒）");
//        list.add("180粒（18粒5盒+3粒30)黑盒");
//        list.add("120粒（18粒3盒+3粒22)黑盒");
//        list.add("90粒（18粒2盒+3粒18盒)黑盒");
//        list.add("270粒（18粒7盒+3粒48)黑盒");
//        list.add("60粒（3粒20盒)黑盒");
//        list.add("0.12克X90粒(3粒装/30盒）");
//        list.add("雅塑90粒（18粒/盒）五盒");
//        list.add("雅塑54粒（18粒/盒）三盒");
//        list.add("雅塑36粒（18粒/盒）二盒");
//        list.add("180粒（18粒5盒+3粒30盒）");
//        list.add("120粒（18粒4盒+3粒16盒）");
//        list.add("90粒（18粒3盒+3粒12盒）");
//        list.add("36粒（18粒1盒+3粒6盒）");
//        list.add("1盒18粒");
//        list.add("5盒90粒");
//        list.add("3粒装6盒18粒（多发3粒装1盒）");
//        list.add("标准装");
//        list.add("艾丽奥利司他胶囊21粒【7粒*3盒】");
//        list.add("【随机发】卷尺一个 ");
//        list.add("一个星期用量7盒装-21粒");
//        list.add("顶俏牌奥利司他】三盒装-24粒");
//        list.add("【4盒一疗程】84粒 1个月量");
//        list.add("包邮】6盒 126粒");
//        list.add("包邮】2盒42粒 半个月用量");
//        list.add("【盒】7粒/1板*1板1盒");
//        list.add("【每日饭后一粒】关注店铺优先发货哦亲-【新人推荐排油体验装】3粒");
//        list.add("0.12g*7粒的2盒");
//        list.add("2盒+1盒排毒养颜70粒");
//        list.add("原品5盒【拍五发六，实发六盒");
//        list.add("【单纯性肥胖】本品+碧生源常菁茶1盒");
//        list.add("减肥掉秤】艾丽84粒+送30粒白芸豆");
//        list.add("舒尔佳奥利司他（7粒）3盒装-1");
//        list.add("72粒*60mg】24天减脂尖货");
//        list.add("减肥福利】每个用户限购一份-超划算】艾丽24粒+下单多得24粒");
//        list.add("60mg*5粒（3盒装）");
//        list.add("艾丽126粒】加送60粒白芸豆");

//        list.stream().forEach((a) -> {
//            Map<String, Long> titleParse = titleParseFx("", a);
//        });
    }

}
