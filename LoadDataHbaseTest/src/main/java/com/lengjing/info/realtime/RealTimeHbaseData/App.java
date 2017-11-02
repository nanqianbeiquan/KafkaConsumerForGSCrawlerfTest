package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;







import org.mortbay.log.Log;

import scala.util.regexp.Base.RegExp;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

/**
 * Hello world!
 *
 */
public class App extends Thread
{
    public static void main( String[] args ) throws Exception
    {
    	String str="{\"inputCompanyName\": \"许昌华丰实业有限公司\", \"Branches\": [], \"taskId\": \"null\", \"Business_Abnormal\": [], \"Administrative_Penalty\": [], \"Chattel_Mortgage\": [], \"liquidation_Information\": [], \"Registered_Info\": [{\"Registered_Info:registrationno\": \"91411023174430174N\", \"Registered_Info:registeredcapital\": \"2,000万元\", \"Registered_Info:legalrepresentative\": \"张焕卿\", \"Registered_Info:businessscope\": \"机械（铸造除外）制造,销售。\", \"Registered_Info:tyshxy_code\": \"91411023174430174N\", \"Registered_Info:approvaldate\": \"2016年7月8日\", \"Registered_Info:residenceaddress\": \"许昌尚集产业集聚区万向路\", \"Registered_Info:validityto\": \"\", \"Registered_Info:establishmentdate\": \"2005年10月24日\", \"Registered_Info:province\": \"河南省\", \"Registered_Info:lastupdatetime\": \"2016-09-06 18:45:02\", \"rowkey\": \"许昌华丰实业有限公司_01_91411023174430174N_\", \"Registered_Info:enterprisetype\": \"有限责任公司(自然人投资或控股)\", \"Registered_Info:registrationinstitution\": \"许昌市许昌县工商行政管理局\", \"Registered_Info:registrationstatus\": \"存续\", \"Registered_Info:enterprisename\": \"许昌华丰实业有限公司\", \"Registered_Info:validityfrom\": \"2007年10月29日\"}], \"Shareholder_Info\": [{\"Shareholder_Info:id\": 1, \"Shareholder_Info:registrationno\": \"91411023174430174N\", \"Shareholder_Info:subscripted_amount\": \"1000万人民币元\", \"Shareholder_Info:subscripted_capital\": \"1000\", \"Shareholder_Info:shareholder_name\": \"张焕卿\", \"rowkey\": \"许昌华丰实业有限公司_04_91411023174430174N_201609061\", \"Shareholder_Info:actualpaid_capital\": \"1000\", \"Shareholder_Info:enterprisename\": \"许昌华丰实业有限公司\", \"Shareholder_Info:subscripted_time\": \"2008年11月9日\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:lastupdatetime\": \"2016-09-06 18:45:02\", \"Shareholder_Info:actualpaid_time\": \"2008年11月9日\", \"Shareholder_Info:shareholder_certificationno\": \"*****\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"1000万人民币元\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 2, \"Shareholder_Info:registrationno\": \"91411023174430174N\", \"Shareholder_Info:subscripted_amount\": \"1000万人民币元\", \"Shareholder_Info:subscripted_capital\": \"1000\", \"Shareholder_Info:shareholder_name\": \"董会涛\", \"rowkey\": \"许昌华丰实业有限公司_04_91411023174430174N_201609062\", \"Shareholder_Info:actualpaid_capital\": \"1000\", \"Shareholder_Info:enterprisename\": \"许昌华丰实业有限公司\", \"Shareholder_Info:subscripted_time\": \"2014年2月24日\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:lastupdatetime\": \"2016-09-06 18:45:02\", \"Shareholder_Info:actualpaid_time\": \"2014年2月24日\", \"Shareholder_Info:shareholder_certificationno\": \"*****\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"1000万人民币元\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}], \"Changed_Announcement\": [{\"Changed_Announcement:id\": 1, \"Changed_Announcement:changedannouncement_after\": \"许昌尚集产业集聚区万向路\", \"Changed_Announcement:changedannouncement_date\": \"2016年7月8日\", \"Changed_Announcement:enterprisename\": \"许昌华丰实业有限公司\", \"Changed_Announcement:lastupdatetime\": \"2016-09-06 18:45:03\", \"rowkey\": \"许昌华丰实业有限公司_05_91411023174430174N_201609061\", \"Changed_Announcement:changedannouncement_events\": \"经营场所\", \"Changed_Announcement:registrationno\": \"91411023174430174N\", \"Changed_Announcement:changedannouncement_before\": \"许昌县张潘镇\"}, {\"Changed_Announcement:id\": 2, \"Changed_Announcement:changedannouncement_after\": \"长期\", \"Changed_Announcement:changedannouncement_date\": \"2016年7月8日\", \"Changed_Announcement:enterprisename\": \"许昌华丰实业有限公司\", \"Changed_Announcement:lastupdatetime\": \"2016-09-06 18:45:03\", \"rowkey\": \"许昌华丰实业有限公司_05_91411023174430174N_201609062\", \"Changed_Announcement:changedannouncement_events\": \"经营期限\", \"Changed_Announcement:registrationno\": \"91411023174430174N\", \"Changed_Announcement:changedannouncement_before\": \"9\"}], \"Spot_Check\": [], \"KeyPerson_Info\": [{\"KeyPerson_Info:keyperson_position\": \"执行董事兼总经理\", \"KeyPerson_Info:lastupdatetime\": \"2016-09-06 18:45:03\", \"KeyPerson_Info:registrationno\": \"91411023174430174N\", \"KeyPerson_Info:keyperson_name\": \"张焕卿\", \"KeyPerson_Info:keyperson_no\": \"1\", \"KeyPerson_Info:id\": 1, \"KeyPerson_Info:enterprisename\": \"许昌华丰实业有限公司\", \"rowkey\": \"许昌华丰实业有限公司_06_91411023174430174N_201609061\"}, {\"KeyPerson_Info:keyperson_position\": \"监事\", \"KeyPerson_Info:lastupdatetime\": \"2016-09-06 18:45:03\", \"KeyPerson_Info:registrationno\": \"91411023174430174N\", \"KeyPerson_Info:keyperson_name\": \"董会涛\", \"KeyPerson_Info:keyperson_no\": \"2\", \"KeyPerson_Info:id\": 2, \"KeyPerson_Info:enterprisename\": \"许昌华丰实业有限公司\", \"rowkey\": \"许昌华丰实业有限公司_06_91411023174430174N_201609062\"}], \"accountId\": \"null\", \"Serious_Violations\": []}";
    	LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
    	
    	
    	/*InputStreamReader inputRead;
		try {
			inputRead = new InputStreamReader(
						new FileInputStream("/home/zyx/hbaseBatch/2016-09-19.log"), "utf-8");
			BufferedReader bufferedReader = new BufferedReader(inputRead);
    		String lineTxt = null;
    		while ((lineTxt = bufferedReader.readLine()) != null) {
    			JSONObject jsonobj=JSONObject.parseObject(lineTxt.replaceAll("\r", "").replaceAll("\n", ""));
    			String companyName=jsonobj.getString("inputCompanyName");
				JSONArray jsonarray=jsonobj.getJSONArray("Registered_Info");
				//Log.info(lineTxt);
				if(jsonarray!=null){
					JSONObject  obj= jsonarray.getJSONObject(0);
	    			if(obj.containsKey("Registered_Info:province") && obj.get("Registered_Info:province").equals("山东省")){
	    				if(obj.containsKey("Registered_Info:registrationno") && obj.get("Registered_Info:registrationno").equals("")){
	    					Log.info(lineTxt);
	    				};
	    			}
				}    			
    		}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/// 考虑到编码格式
    		
    		/*String str="{\"inputCompanyName\": \"金坛市三胜电子有限公司\", \"taskId\": \"null\", \"accountId\": \"null\"}";
    		JSONObject jsonobj=JSONObject.parseObject(str);
			String companyName=jsonobj.getString("inputCompanyName");
			JSONArray jsonarray=jsonobj.getJSONArray("Registered_Info");
			System.out.println(jsonarray!=null);*/
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
