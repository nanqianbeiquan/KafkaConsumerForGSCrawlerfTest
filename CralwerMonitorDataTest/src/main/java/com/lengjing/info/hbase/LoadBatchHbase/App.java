package com.lengjing.info.hbase.LoadBatchHbase;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {
    	/*JSONObject jsonobj=JSONObject.parseObject(str);
    	JSONArray jsonarray=jsonobj.getJSONArray("KeyPerson_Info");
    	for(int i=0;i<jsonarray.size();i++){
    		JSONObject obj=jsonarray.getJSONObject(i);
    		System.out.println(obj);
			Iterator it= obj.keySet().iterator();
			while(it.hasNext()){
				String key = (String) it.next();  
                String value = obj.getString(key); 
                System.out.println(key+"---"+value);
                System.out.println("----");
			}

    	}*/
    	
    	/*SimpleDateFormat sdf = new SimpleDateFormat("HH");
		String date=sdf.format(new Date()); 
    	System.out.println(date);*/
    	String str="{\"inputCompanyName\": \"上海泉江印铁有限公司\", \"Changed_Announcement\": [], \"KeyPerson_Info\": [{\"KeyPerson_Info:keyperson_position\": \"监事\", \"KeyPerson_Info:registrationno\": \"913101137824371694\", \"KeyPerson_Info:keyperson_name\": \"许英裕\", \"KeyPerson_Info:id\": \"1\", \"KeyPerson_Info:enterprisename\": \"上海泉江印铁有限公司\", \"rowkey\": \"上海泉江印铁有限公司_06_913101137824371694_201704251\"}, {\"KeyPerson_Info:keyperson_position\": \"执行董事\", \"KeyPerson_Info:registrationno\": \"913101137824371694\", \"KeyPerson_Info:keyperson_name\": \"苏承魁\", \"KeyPerson_Info:id\": \"2\", \"KeyPerson_Info:enterprisename\": \"上海泉江印铁有限公司\", \"rowkey\": \"上海泉江印铁有限公司_06_913101137824371694_201704252\"}, {\"KeyPerson_Info:keyperson_position\": \"执行董事\", \"KeyPerson_Info:registrationno\": \"913101137824371694\", \"KeyPerson_Info:keyperson_name\": \"苏承魁\", \"KeyPerson_Info:id\": \"3\", \"KeyPerson_Info:enterprisename\": \"上海泉江印铁有限公司\", \"rowkey\": \"上海泉江印铁有限公司_06_913101137824371694_201704253\"}], \"Registered_Info\": [{\"Registered_Info:registrationno\": \"913101137824371694\", \"Registered_Info:registeredcapital\": \"4500万人民币\", \"Registered_Info:validityfrom\": \"2005年11月9日\", \"Registered_Info:legalrepresentative\": \"苏承魁\", \"Registered_Info:tyshxy_code\": \"913101137824371694\", \"Registered_Info:approvaldate\": \"2005年11月9日\", \"Registered_Info:residenceaddress\": \"上海市宝山区春和路406号\", \"Registered_Info:validityto\": \"2025年11月8日\", \"Registered_Info:establishmentdate\": \"2005年11月9日\", \"Registered_Info:province\": \"上海市\", \"rowkey\": \"上海泉江印铁有限公司_01_913101137824371694_\", \"Registered_Info:enterprisetype\": \"有限责任公司\", \"Registered_Info:registrationinstitution\": \"宝山区市场监督管理局\", \"Registered_Info:businessscope\": \"包装装潢印刷；金属涂层；金属制品加工。\n【依法须经批准的项目，经相关部门批准后方可开展经营活动】\", \"Registered_Info:registrationstatus\": \"存续（在营、开业、在册）\", \"Registered_Info:enterprisename\": \"上海泉江印铁有限公司\"}], \"Shareholder_Info\": [{\"Shareholder_Info:id\": 1, \"Shareholder_Info:registrationno\": \"913101137824371694\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"450\"}], \"Shareholder_Info:subscripted_amount\": \"450\", \"Shareholder_Info:subscripted_capital\": \"450\", \"Shareholder_Info:shareholder_name\": \"张裘萍\", \"rowkey\": \"上海泉江印铁有限公司_04_913101137824371694_201704251\", \"Shareholder_Info:actualpaid_capital\": \"450.0\", \"Shareholder_Info:enterprisename\": \"上海泉江印铁有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2013年5月17日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"130\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2013年5月17日\", \"实缴出资额（万元）\": \"130\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2010年6月7日\", \"实缴出资额（万元）\": \"150\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2007年6月5日\", \"实缴出资额（万元）\": \"170\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 2, \"Shareholder_Info:registrationno\": \"913101137824371694\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"450\"}], \"Shareholder_Info:subscripted_amount\": \"450\", \"Shareholder_Info:subscripted_capital\": \"450\", \"Shareholder_Info:shareholder_name\": \"许英裕\", \"rowkey\": \"上海泉江印铁有限公司_04_913101137824371694_201704252\", \"Shareholder_Info:actualpaid_capital\": \"450.0\", \"Shareholder_Info:enterprisename\": \"上海泉江印铁有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2007年6月5日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"170\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2007年6月5日\", \"实缴出资额（万元）\": \"170\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2010年6月7日\", \"实缴出资额（万元）\": \"150\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2013年5月17日\", \"实缴出资额（万元）\": \"130\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 3, \"Shareholder_Info:registrationno\": \"913101137824371694\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"1800\"}], \"Shareholder_Info:subscripted_amount\": \"1800\", \"Shareholder_Info:subscripted_capital\": \"1800\", \"Shareholder_Info:shareholder_name\": \"苏承魁\", \"rowkey\": \"上海泉江印铁有限公司_04_913101137824371694_201704253\", \"Shareholder_Info:actualpaid_capital\": \"1800.0\", \"Shareholder_Info:enterprisename\": \"上海泉江印铁有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2007年6月5日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"680\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2007年6月5日\", \"实缴出资额（万元）\": \"680\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2013年5月17日\", \"实缴出资额（万元）\": \"520\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2010年6月7日\", \"实缴出资额（万元）\": \"600\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 4, \"Shareholder_Info:registrationno\": \"913101137824371694\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"1800\"}], \"Shareholder_Info:subscripted_amount\": \"1800\", \"Shareholder_Info:subscripted_capital\": \"1800\", \"Shareholder_Info:shareholder_name\": \"苏汉昌\", \"rowkey\": \"上海泉江印铁有限公司_04_913101137824371694_201704254\", \"Shareholder_Info:actualpaid_capital\": \"1800.0\", \"Shareholder_Info:enterprisename\": \"上海泉江印铁有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2007年6月5日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"680\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2007年6月5日\", \"实缴出资额（万元）\": \"680\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2013年5月17日\", \"实缴出资额（万元）\": \"520\", \"实缴出资方式\": \"货币\"}, {\"实缴出资日期\": \"2010年6月3日\", \"实缴出资额（万元）\": \"600\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}], \"taskId\": \"null\", \"accountId\": \"null\"}";
    	LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
    	//String str="{\"inputCompanyName\": \"上海富礼电子有限公司\", \"Changed_Announcement\": [{\"Changed_Announcement:id\": \"1\", \"Changed_Announcement:changedannouncement_after\": \"其他机械设备及电子产品批发\", \"Changed_Announcement:changedannouncement_date\": \"2014年11月11日\", \"Changed_Announcement:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_05_310118002585477_201703091\", \"Changed_Announcement:changedannouncement_events\": \"行业代码变更\", \"Changed_Announcement:registrationno\": \"310118002585477\", \"Changed_Announcement:changedannouncement_before\": \"其他机械设备及电子产品批发\"}, {\"Changed_Announcement:id\": \"2\", \"Changed_Announcement:changedannouncement_after\": \"2014-10-17章程备案\", \"Changed_Announcement:changedannouncement_date\": \"2014年11月11日\", \"Changed_Announcement:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_05_310118002585477_201703092\", \"Changed_Announcement:changedannouncement_events\": \"章程备案\", \"Changed_Announcement:registrationno\": \"310118002585477\", \"Changed_Announcement:changedannouncement_before\": \"无\"}, {\"Changed_Announcement:id\": \"3\", \"Changed_Announcement:changedannouncement_after\": \"无\", \"Changed_Announcement:changedannouncement_date\": \"2014年11月11日\", \"Changed_Announcement:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_05_310118002585477_201703093\", \"Changed_Announcement:changedannouncement_events\": \"许可经营项目变更\", \"Changed_Announcement:registrationno\": \"310118002585477\", \"Changed_Announcement:changedannouncement_before\": \"无\"}, {\"Changed_Announcement:id\": \"4\", \"Changed_Announcement:changedannouncement_after\": \"电子产品及元器件、数码产品、仪器仪表、计算机软硬件的研究、开发及销售，电子、通讯、计算机、自动化专业技术领域内的技术服务、技术咨询，销售通讯器材及设备、电器产品、电信器材、电气设备、照明器材、计算机及配件（除计算机信息系统安全专用产品）。\", \"Changed_Announcement:changedannouncement_date\": \"2014年11月11日\", \"Changed_Announcement:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_05_310118002585477_201703094\", \"Changed_Announcement:changedannouncement_events\": \"一般经营项目变更\", \"Changed_Announcement:registrationno\": \"310118002585477\", \"Changed_Announcement:changedannouncement_before\": \"电子产品及元器件、数码产品、仪器仪表、计算机软硬件的研究、开发及销售，电子、通讯、计算机、自动化专业技术领域内的技术服务、技术咨询，销售通讯器材及设备、电器产品、电信器材、电气设备、照明器材、计算机及配件（除计算机信息系统安全专用产品）。\"}, {\"Changed_Announcement:id\": \"5\", \"Changed_Announcement:changedannouncement_after\": \"电子产品及元器件、数码产品、仪器仪表、计算机软硬件的研究、开发及销售，电子、通讯、计算机、自动化专业技术领域内的技术服务、技术咨询，销售通讯器材及设备、电器产品、电信器材、电气设备、照明器材、计算机及配件（除计算机信息系统安全专用产品）。【依法须经批准的项目，经相关部门批准后方可开展经营活动】\", \"Changed_Announcement:changedannouncement_date\": \"2014年11月11日\", \"Changed_Announcement:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_05_310118002585477_201703095\", \"Changed_Announcement:changedannouncement_events\": \"经营范围变更\", \"Changed_Announcement:registrationno\": \"310118002585477\", \"Changed_Announcement:changedannouncement_before\": \"电子产品及元器件、数码产品、仪器仪表、计算机软硬件的研究、开发及销售，电子、通讯、计算机、自动化专业技术领域内的技术服务、技术咨询，销售通讯器材及设备、电器产品、电信器材、电气设备、照明器材、计算机及配件（除计算机信息系统安全专用产品）。【企业经营涉及行政许可的，凭许可证件经营】\"}], \"KeyPerson_Info\": [{\"KeyPerson_Info:keyperson_position\": \"执行董事兼总经理\", \"KeyPerson_Info:registrationno\": \"310118002585477\", \"KeyPerson_Info:keyperson_name\": \"郑雪梅\", \"KeyPerson_Info:id\": \"1\", \"KeyPerson_Info:enterprisename\": \"上海富礼电子有限公司\", \"rowkey\": \"上海富礼电子有限公司_06_310118002585477_201703091\"}, {\"KeyPerson_Info:keyperson_position\": \"监事\", \"KeyPerson_Info:registrationno\": \"310118002585477\", \"KeyPerson_Info:keyperson_name\": \"程红霞\", \"KeyPerson_Info:id\": \"2\", \"KeyPerson_Info:enterprisename\": \"上海富礼电子有限公.\", \"rowkey\": \"上海富礼电子有限公司_06_310118002585477_201703092\"}], \"Registered_Info\": [{\"Registered_Info:registrationno\": \"310118002585477\", \"Registered_Info:registeredcapital\": \"50万人民币\", \"Registered_Info:validityfrom\": \"2011年1月5日\", \"Registered_Info:legalrepresentative\": \"郑雪梅\", \"Registered_Info:approvaldate\": \"2011年1月5日\", \"Registered_Info:residenceaddress\": \"青浦区新达路1218号1幢2层A区246室                                                                                                                                                                                                                                                                                                                                                            \", \"Registered_Info:validityto\": \"2021年1月4日\", \"Registered_Info:establishmentdate\": \"2011年1月5日\", \"Registered_Info:province\": \"上海市\", \"Registered_Info:zch\": \"310118002585477\", \"rowkey\": \"上海富礼电子有限公司_01_310118002585477_\", \"Registered_Info:enterprisetype\": \"有限责任公司\", \"Registered_Info:registrationinstitution\": \"青浦区市场监督管理局\", \"Registered_Info:businessscope\": \"电子产品及元器件、数码产品、仪器仪表、计算机软硬件的研究、开发及销售，电子、通讯、计算机、自动化专业技术领域内的技术服务、技术咨询，销售通讯器材及设备、电器产品、电信器材、电气设备、照明器材、计算机及配件（除计算机信息系统安全专用产品）。\r\n【依法须经批准的项目，经相关部门批准后方可开展经营活动】\", \"Registered_Info:registrationstatus\": \"存续（在营、开业、在册）\", \"Registered_Info:enterprisename\": \"上海富礼电子有限公司\"}], \"Shareholder_Info\": [{\"Shareholder_Info:id\": 1, \"Shareholder_Info:registrationno\": \"310118002585477\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"5\"}], \"Shareholder_Info:subscripted_amount\": \"5\", \"Shareholder_Info:shareholder_name\": \"程红霞\", \"rowkey\": \"上海富礼电子有限公司_04_310118002585477_201703091\", \"Shareholder_Info:actualpaid_capital\": \"\", \"Shareholder_Info:enterprisename\": \"上海富礼电子有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2010年12月31日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"5\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2010年12月31日\", \"实缴出资额（万元）\": \"5\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": 2, \"Shareholder_Info:registrationno\": \"310118002585477\", \"Shareholder_Info:rjmx\": [{\"认缴出资日期\": \"\", \"认缴出资方式\": \"货币\", \"认缴出资额（万元）\": \"45\"}], \"Shareholder_Info:subscripted_amount\": \"45\", \"Shareholder_Info:shareholder_name\": \"郑雪梅\", \"rowkey\": \"上海富礼电子有限公司_04_310118002585477_201703092\", \"Shareholder_Info:actualpaid_capital\": \"\", \"Shareholder_Info:enterprisename\": \"上海富礼电子有限公司\", \"Shareholder_Info:subscripted_time\": \"\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:actualpaid_time\": \"2010年12月31日\", \"Shareholder_Info:shareholder_certificationno\": \"非公示项\", \"Shareholder_Info:subscripted_method\": \"货币\", \"Shareholder_Info:actualpaid_amount\": \"5\", \"Shareholder_Info:actualpaid_method\": \"货币\", \"Shareholder_Info:shareholder_details\": \"查看\", \"Shareholder_Info:sjmx\": [{\"实缴出资日期\": \"2010年12月31日\", \"实缴出资额（万元）\": \"5\", \"实缴出资方式\": \"货币\"}], \"Shareholder_Info:shareholder_type\": \"自然人股东\"}], \"taskId\": \"null\", \"accountId\": \"null\"}";
    	//LoadHbaseData.loadData(str.replaceAll("\r", "").replaceAll("\n", ""));
    	/*SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	System.out.println(sdf.format(new Date()));*/
        /* Set set=new HashSet();
         set.add("1");
         set.add("2");
         set.add("3");
         set.add("4");
         Set set2=new HashSet();
         set2.add("1");
         set2.add("5");
         set2.add("3");
         set2.add("4");
         System.out.println(set.contains("5"));*/
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