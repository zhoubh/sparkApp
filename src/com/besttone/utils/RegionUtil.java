package com.besttone.utils;

import java.util.HashMap;
import java.util.Map;

public class RegionUtil {
	public static Map regionMap = new HashMap(); 
	
	static{
		regionMap.put("中国", "000000");
		regionMap.put("北京", "110000");
		regionMap.put("天津", "120000");
		regionMap.put("河北", "130000");
		regionMap.put("山西", "140000");
		regionMap.put("内蒙", "150000");
		regionMap.put("辽宁", "210000");
		regionMap.put("吉林", "220000");
		regionMap.put("黑龙", "230000");
		regionMap.put("上海", "310000");
		regionMap.put("江苏", "320000");
		regionMap.put("浙江", "330000");
		regionMap.put("安徽", "340000");
		regionMap.put("福建", "350000");
		regionMap.put("江西", "360000");
		regionMap.put("山东", "370000");
		regionMap.put("河南", "410000");
		regionMap.put("湖北", "420000");
		regionMap.put("湖南", "430000");
		regionMap.put("广东", "440000");
		regionMap.put("广西", "450000");
		regionMap.put("海南", "460000");
		regionMap.put("重庆", "500000");
		regionMap.put("四川", "510000");
		regionMap.put("贵州", "520000");
		regionMap.put("云南", "530000");
		regionMap.put("西藏", "540000");
		regionMap.put("陕西", "610000");
		regionMap.put("甘肃", "620000");
		regionMap.put("青海", "630000");
		regionMap.put("宁夏", "640000");
		regionMap.put("新疆", "650000");
		regionMap.put("台湾", "710000");
		regionMap.put("香港", "810000");
		regionMap.put("澳门", "820000");

	}
	
	public static String  getRegionCode(String regionName){
		String regionCode = (String)regionMap.get(regionName);
		
		if(regionCode == null){
			return 	"000000";
		}else{
			return 	regionCode;	
		}
		
	}

}