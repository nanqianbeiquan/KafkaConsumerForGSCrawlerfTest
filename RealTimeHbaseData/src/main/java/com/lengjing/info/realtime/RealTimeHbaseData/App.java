package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jettison.json.JSONObject;

import scala.util.regexp.Base.RegExp;

/**
 * Hello world!
 *
 */
public class App extends Thread
{
    public static void main( String[] args ) throws Exception
    {
    	String str="{\"inputCompanyName\": \"内蒙古霖泰酒业有限公司\", \"Branches\": [], \"taskId\": \"456\", \"Business_Abnormal\": [], \"Administrative_Penalty\": [], \"Chattel_Mortgage\": [], \"liquidation_Information\": [], \"Registered_Info\": [{\"Registered_Info:registrationno\": \"911501056865458646\", \"Registered_Info:registeredcapital\": \"200万人民币元\", \"Registered_Info:legalrepresentative\": \"钱曙光\", \"Registered_Info:businessscope\": \"许可经营项目：批发兼零售：预包装食品（主营酒类）、不含乳制品（该项目有效期至２０１6年5月4日）。一般经营项目：无(依法须经批准的项目，经相关部门批准后方可开展经营活动)〓\", \"Registered_Info:tyshxy_code\": \"911501056865458646\", \"Registered_Info:approvaldate\": \"2015年11月12日\", \"Registered_Info:businessplace\": \"内蒙古自治区呼和浩特市赛罕区大学东路桥华世纪村B区展西路东商业楼1-2层11\", \"Registered_Info:validityto\": \"2029年05月11日\", \"Registered_Info:establishmentdate\": \"2009年05月12日\", \"Registered_Info:province\": \"内蒙古自治区\", \"Registered_Info:zch\": \"150105000032432\", \"Registered_Info:lastupdatetime\": \"2016-09-12 15:50:50\", \"rowkey\": \"内蒙古霖泰酒业有限公司_01_911501056865458646_\", \"Registered_Info:enterprisetype\": \"有限责任公司(自然人投资或控股)\", \"Registered_Info:registrationinstitution\": \"呼和浩特市工商行政管理局赛罕分局\", \"Registered_Info:registrationstatus\": \"存续\", \"Registered_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Registered_Info:validityfrom\": \"2009年05月12日\"}], \"Shareholder_Info\": [{\"Shareholder_Info:id\": 1, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 20, \"Shareholder_Info:shareholder_name\": \"詹友萍\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609121\", \"Shareholder_Info:actualpaid_amount\": 20, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 2, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 20, \"Shareholder_Info:shareholder_name\": \"邹梦琪\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609122\", \"Shareholder_Info:actualpaid_amount\": 20, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 3, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 30, \"Shareholder_Info:shareholder_name\": \"解淑琴\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609123\", \"Shareholder_Info:actualpaid_amount\": 0, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 4, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 10, \"Shareholder_Info:shareholder_name\": \"刘海燕\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609124\", \"Shareholder_Info:actualpaid_amount\": 10, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 5, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 40, \"Shareholder_Info:shareholder_name\": \"刘谨赫\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609125\", \"Shareholder_Info:actualpaid_amount\": 40, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 6, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 30, \"Shareholder_Info:shareholder_name\": \"邢素芝\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 19, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 19, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609126\", \"Shareholder_Info:actualpaid_amount\": 30, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 7, \"Shareholder_Info:registrationno\": \"911501056865458646\", \"Shareholder_Info:subscripted_amount\": 50, \"Shareholder_Info:shareholder_name\": \"钱曙光\", \"Shareholder_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Shareholder_Info:subscripted_time\": \"Apr 15, 2010 12:00:00 AM\", \"Shareholder_Info:shareholder_certificationtype\": \"\", \"Shareholder_Info:lastupdatetime\": \"2016-09-12 15:51:04\", \"Shareholder_Info:actualpaid_time\": \"Apr 15, 2010 12:00:00 AM\", \"rowkey\": \"内蒙古霖泰酒业有限公司_04_911501056865458646_201609127\", \"Shareholder_Info:actualpaid_amount\": 50, \"Shareholder_Info:actualpaid_method\": \"货币出资\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}], \"Changed_Announcement\": [{\"Changed_Announcement:id\": 1, \"Changed_Announcement:changedannouncement_after\": \"解淑琴,刘海燕,刘谨赫,钱曙光,邢素芝,詹友萍,邹梦琪\", \"Changed_Announcement:changedannouncement_date\": \"Sep 25, 2014 12:00:00 AM\", \"Changed_Announcement:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"Changed_Announcement:lastupdatetime\": \"2016-09-12 15:51:13\", \"rowkey\": \"内蒙古霖泰酒业有限公司_05_911501056865458646_201609121\", \"Changed_Announcement:changedannouncement_events\": \"自然人股东\", \"Changed_Announcement:registrationno\": \"911501056865458646\", \"Changed_Announcement:changedannouncement_before\": \"高登辉,郭学利,刘海燕,刘谨赫,钱曙光,王月玲,詹友萍\"}], \"Spot_Check\": [], \"KeyPerson_Info\": [{\"KeyPerson_Info:keyperson_position\": \"执行董事兼经理\", \"KeyPerson_Info:lastupdatetime\": \"2016-09-12 15:53:06\", \"KeyPerson_Info:registrationno\": \"911501056865458646\", \"KeyPerson_Info:keyperson_name\": \"钱曙光\", \"KeyPerson_Info:keyperson_no\": \"1\", \"KeyPerson_Info:id\": 1, \"KeyPerson_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"rowkey\": \"内蒙古霖泰酒业有限公司_06_911501056865458646_201609121\"}, {\"KeyPerson_Info:keyperson_position\": \"监事\", \"KeyPerson_Info:lastupdatetime\": \"2016-09-12 15:53:06\", \"KeyPerson_Info:registrationno\": \"911501056865458646\", \"KeyPerson_Info:keyperson_name\": \"刘谨赫\", \"KeyPerson_Info:keyperson_no\": \"2\", \"KeyPerson_Info:id\": 2, \"KeyPerson_Info:enterprisename\": \"内蒙古霖泰酒业有限公司\", \"rowkey\": \"内蒙古霖泰酒业有限公司_06_911501056865458646_201609122\"}], \"accountId\": \"123\", \"Serious_Violations\": []}";
    	LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
    	/*for(int i=14;i<24;i++){
    		InputStreamReader inputRead = new InputStreamReader(
    				new FileInputStream("d:\\yubing.lei\\Desktop\\hivetest\\2016-09-05\\"+i), "utf-8");// 考虑到编码格式
    		BufferedReader bufferedReader = new BufferedReader(inputRead);
    		String lineTxt = null;
    		while ((lineTxt = bufferedReader.readLine()) != null) {
    	    	LoadHbaseData.loadData(lineTxt.replaceAll("\r", "").replaceAll("\n", ""));
    			
    		}
    	}*/
    	
    		/*InputStreamReader inputRead = new InputStreamReader(
    				new FileInputStream("d:\\yubing.lei\\Desktop\\hivetest\\2016-09-06\\0"), "utf-8");// 考虑到编码格式
    		BufferedReader bufferedReader = new BufferedReader(inputRead);
    		String lineTxt = null;
    		while ((lineTxt = bufferedReader.readLine()) != null) {
    	    	LoadHbaseData.loadData(lineTxt.replaceAll("\r", "").replaceAll("\n", ""));
    			
    	}*/
    	/*for(int i=0;i<24;i++){
    		InputStreamReader inputRead = new InputStreamReader(
    				new FileInputStream("d:\\yubing.lei\\Desktop\\hivetest\\2016-09-07\\"+i), "utf-8");// 考虑到编码格式
    		BufferedReader bufferedReader = new BufferedReader(inputRead);
    		String lineTxt = null;
    		while ((lineTxt = bufferedReader.readLine()) != null) {
    	    	LoadHbaseData.loadData(lineTxt.replaceAll("\r", "").replaceAll("\n", ""));
    			
    		}
    	}*/
    	
    	/*try {
			//LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
    		JSONObject	jsonobj = new JSONObject(str);
    		org.codehaus.jettison.json.JSONArray jsonarray=jsonobj.getJSONArray("Registered_Info");
    		for(int i=0;i<jsonarray.length();i++){
				JSONObject t = jsonarray.getJSONObject(i);
				Iterator<Object> it=t.keys();
				while(it.hasNext()){
					String key=it.next().toString();
					String value=t.getString(key);
					System.out.println(key+"---"+value);
				}
    		}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	/*String regex="([\\u4e00-\\u9fa5]+)";
    	String str="30.000000万人民币";
    	Matcher matcher = Pattern.compile(regex).matcher(str);
    	if(matcher.find()){
    		System.out.println(matcher.group(0));
    	}*/
    	
    }
}
