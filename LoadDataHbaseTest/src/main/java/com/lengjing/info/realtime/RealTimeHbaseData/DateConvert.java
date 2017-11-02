package com.lengjing.info.realtime.RealTimeHbaseData;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateConvert{
	
	
	public String evaluate(String changeDt) {
		String date="";
		SimpleDateFormat sdf1=new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2=new SimpleDateFormat("yyyy年M月d日");
		SimpleDateFormat sdf3=new SimpleDateFormat("yyyyMMdd");
		if(changeDt!=null && !changeDt.equals("null") && !changeDt.equals("") && changeDt.indexOf("年")>0){
			try{
				date=sdf1.format(sdf2.parse(changeDt)); 
			}catch (Exception e){
				date=changeDt;
			}
			return date;
		}else if(changeDt.indexOf("年")<0){
			if(changeDt.indexOf("-")>0){
				return changeDt;
			}else{
				try{
					date=sdf1.format(sdf3.parse(changeDt)); 
				}catch (Exception e){
					date=changeDt;
				}
				return date;
			}
		}else{
			return "";
		}
		
	}
	public static void main(String[] args) throws ParseException {
		String changeDt="2014-05-28";
		DateConvert dconvet=new DateConvert();
		System.out.println(""+dconvet.evaluate(changeDt));
	}

}
