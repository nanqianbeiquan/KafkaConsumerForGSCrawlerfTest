package com.lengjing.info.realtime.RealTimeHbaseData;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test extends Thread{
	
	 
	
	@Override
	public void run() {
		
		
	}
	public static void main(String[] args) {
		//String regex="[\u4e00-\u9fa5]+";
		String data="2000.45万元";
		//String money=data.substring(data.indexOf(str), endIndex)
		String regex="[\u4e00-\u9fa5]+";
    	Matcher matcher = Pattern.compile(regex).matcher("(美元)2000.45万元");
    	if(matcher.find()){
    	 System.out.println(matcher.group(0).replaceAll("[^\u4e00-\u9fa5]",""));
    	}
				String str = "(美元)2000.45万元";  
        String reg = "[^\u4e00-\u9fa5]";  
        str = str.replaceAll(reg,"").replaceAll(matcher.group(0), "");  
        System.out.println(str);
	}

}
