package com.besttone.utils;

import com.besttone.ip.IpSearch;
import com.besttone.utils.LatnUtil;
import com.besttone.utils.RegionUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Ander on 2016/3/21.
 */
public class BaseMr {
    static List<String> zcityList = new ArrayList<String>();
    static List<String> zcontyList = new ArrayList<String>();
    String line;

    static {
        zcityList.add("澳门");
        zcityList.add("台湾");
        zcityList.add("香港");
        zcityList.add("海南");
        zcityList.add("天津");
        zcityList.add("北京");
        zcityList.add("重庆");

        zcontyList.add("济源");
        zcontyList.add("仙桃");
        zcontyList.add("潜江");
        zcontyList.add("天门");
        zcontyList.add("神农架");
        zcontyList.add("阿拉尔");
        zcontyList.add("石河子");
        /* String[] zcity={"台湾","香港","澳门","海南","天津","北京","重庆"};
            String[] zconty={"济源","仙桃","潜江","天门","神农架","阿拉尔","石河子"};*/
    }


    public String sub(String str) {
        String ret = "no" + str;

        try {
            if (line.contains("\"" + str + "\"")) {
                int start = line.indexOf("\"" + str + "\":\"") + 4 + str.length();
                int end = start + line.substring(start).indexOf("\"");
                ret = line.substring(start, end);
            } else if (line.contains("\\\"" + str + "\\\":\\\"")) {
                int start = line.indexOf("\\\"" + str + "\\\":\\\"") + 7 + str.length();
                int end = start + line.substring(start).indexOf("\\\"");
                ret = line.substring(start, end);
            }

        } catch (Exception e) {
            System.out.println(line+"***"+str);
        }
        return ret;
    }

    public Map getSubMap() {
        Map subMap = new HashMap();
        //时间
        String time = sub("logid");
        String htime;
        String rtime=sub("requesttime");

        String day_id = "return";
        String hour_id = "return";
        String min_id="return";
        String sec_id="return";
        if (time.length() > 9) {
            day_id = time.substring(0, 8);
            hour_id = time.substring(8, 10);
            min_id=time.substring(10,12);
            sec_id=time.substring(12,14);
        }else if (time.equals("nologid")&&!rtime.equals("norequesttime")) {
            rtime=rtime.replace("-","").replace(":","").replace(" ","");
            day_id = rtime.substring(0, 8);
            hour_id = rtime.substring(8, 10);
            min_id=rtime.substring(10,12);
            sec_id=rtime.substring(12,14);

        }else if (line.startsWith("20")){
            htime = line.replace("-","").replace(":","").replace(" ","");
            if (htime.length()>10) {
                day_id = htime.substring(0, 8);
                hour_id = htime.substring(8, 10);
                min_id=htime.substring(10,12);
                sec_id=htime.substring(12,14);
            }
        }

        //地区编码
        IpSearch finder = IpSearch.getInstance();
        //String cityCode = sub("cityCode").equals("nocityCode") ? "000000" : sub("cityCode");

        String conty = "noconty";
        String latnCode = "nolatnCode";
        String regionCode = "noregionCode";
        String city = "nocity";
        String province = "noprovince";
        String requestIp = sub("requestip");
       //System.out.println("requestIp");
        String adress;
        if (requestIp.matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) {

            adress = finder.Get(requestIp).toString().trim().replaceAll("\t", "");
            String[] splits = adress.split("\\|");

            if (splits.length == 11) {
                province = splits[2];
                city = splits[3];
                conty = splits[4];
            }
            if (zcityList.contains(province)) {
                city = province;
            } else if (zcontyList.contains(conty)) {
                city = conty;
            } else if ("".equals(city)) {
                city = province;
            }
            if (province.length() > 2) {
                province = province.substring(0, 2);
            }

            regionCode = RegionUtil.getRegionCode(province);
            latnCode = LatnUtil.getRegionCode(city);
        }

        //imei
        String imei;
        if (!sub("imei").equals("noimei"))
            imei = sub("imei");
        else {
            if (!sub("ashwid").equals("noashwid")){
                imei = sub("ashwid");
            }else{
                if (!sub("client_mark_id").equals("noclient_mark_id")){
                    imei = sub("client_mark_id");
                }else{
                    imei="noimei";
                }
            }
        }
           /* imei = sub("ashwid");
        else if (!sub("client_mark_id").equals("noclient_mark_id"))
            imei = sub("client_mark_id");
        else imei = "noimei";*/

        //channelno
        String channelno = sub("channelno");
        if (!channelno.equals("client")&&!channelno.matches("\\d+"))
        channelno="nochannelno";

        //id
        String id = sub("id").replace("-", "");

        //requesttype
        String requesttype = sub("requesttype");



        if (!requesttype.matches("\\d+"))
            requesttype="norequesttype";

        //subjectNum

        String sjNum = sub("subjectNum").replace("-", "");
        if (requesttype.equals("64")){
            if (id.equals("noid")){
                sjNum="nosubjectNum";
            }else {
                sjNum=id.replace("-", "");
            }
        }
        if (!sjNum.equals("nosubjectNum")){

        int sl = sjNum.length();
        String s4=null;
        String s3=null;
        //预处理
        if(sl>4){
            s4= sjNum.substring(0, 4);
            s3= sjNum.substring(0, 3);
        }
        if (sjNum.startsWith("+86") || sjNum.startsWith("086"))
            sjNum = sjNum.substring(3);
        else if (sjNum.startsWith("0086"))
            sjNum = sjNum.substring(4);
        //去掉特定意义符号后，过滤乱码，匹配规则
        if (!sjNum.matches("^\\d+$"))
            sjNum = "return";

       /*  //过滤规则
        if ((sjNum.startsWith("400")||sjNum.startsWith("800"))&&sl!=10) {
            sjNum = "return";
        } else if (sjNum.indexOf("1") != 0 && sjNum.indexOf("0") != 0 && sl > 10) {
            sjNum = "return";
        } else if (sl < 3 || sl == 4) {
            sjNum = "return";
        } *//*else if (sl == 10) {
                if (s3 != "400" && s3 != "800") {
                    sjNum = "return";
                }else if (s4 == "4005" || s4 == "4002" || s4 == "4003"){
                    sjNum="return";
                }
        }*//*
        else if (sl == 3 && sjNum.indexOf("1") != 0) {
            sjNum = "return";
        } else if (sl == 5 || sl == 6) {
            if (!(sjNum.startsWith("1") || sjNum.startsWith("9"))) {
                sjNum = "return";
            }
        } else if (sjNum.startsWith("0")&&sl != 11 && sl != 12) {
            sjNum = "return";
        }else if (sl==7||sl==8||sl==9){
            sjNum="return";
        }*/

            //过滤规则
            if ((sjNum.startsWith("400")||sjNum.startsWith("800"))&&sl!=10) {
                sjNum = "return";
            }
            if (sjNum.indexOf("1") != 0 && sjNum.indexOf("0") != 0 && sl > 10) {
                sjNum = "return";
            }
            if (sl < 3 || sl == 4) {
                sjNum = "return";
            }
            if (sl == 10) {
                if (!s3 .equals("400")  &&  !s3 .equals("800")) {
                    sjNum = "return";
                }
                if (s4 .equals("4002") || s4 .equals("4003") || s4 .equals("4005")){
                    sjNum="return";
                }
            }
             if (sl == 3 && sjNum.indexOf("1") != 0) {
                sjNum = "return";
            }
            if (sl == 5 || sl == 6) {
                if (!(sjNum.startsWith("1") || sjNum.startsWith("9"))) {
                    sjNum = "return";
                }
            }
            if (sjNum.startsWith("0")&&sl != 11 && sl != 12) {
                sjNum = "return";
            }
            if (sl==7||sl==8||sl==9){
                sjNum="return";
            }

        }
        else sjNum="return";





        subMap.put("regionCode", regionCode);
        subMap.put("latnCode", latnCode);
        subMap.put("day_id", day_id);
        subMap.put("hour_id", hour_id);
        subMap.put("min_id",min_id);
        subMap.put("sec_id",sec_id);
        //subMap.put("cityCode", cityCode);
        subMap.put("imei", imei);
        subMap.put("channelno", channelno);
        subMap.put("requesttype", requesttype);
        subMap.put("subjectNum",sjNum);


        return subMap;


    }

    /*public String matchNum(String str) {
        if (!str.equals("no" + str) && !str.matches("^\\d+$"))
            return "no" + str;
        else return str;
    }*/



    public BaseMr(String line) {
        this.line = line;

    }
}