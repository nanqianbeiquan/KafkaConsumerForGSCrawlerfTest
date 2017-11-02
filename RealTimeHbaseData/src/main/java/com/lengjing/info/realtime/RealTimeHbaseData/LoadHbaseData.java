package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.File;
import java.io.FileWriter;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.log.Log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.net.update.UpdateNeo4j;


public class LoadHbaseData {
	public static void loadData(String json) throws Exception{
		String tableName="GS";
		HbaseDaoImpl dao=new HbaseDaoImpl();
		DateConvert datautil=new DateConvert();
		Parsemoney parsemoney=new Parsemoney();
		String searchKeyname="";
		String searchShareholdername="";
		String companyName="";
		HttpConnectHelper connect=new HttpConnectHelper();
		List<String> listPerson=new ArrayList<String>();
		List<String> listShare=new ArrayList<String>();
		List<String> oldListShare=new ArrayList<String>();
		try {
	    	JSONObject jsonobj=JSONObject.parseObject(json);
			companyName=jsonobj.getString("inputCompanyName");
			/****
			 * 行政处罚  是通过行政处罚编号
			 */
			if(jsonobj.containsKey("Administrative_Penalty")){
				JSONArray business=jsonobj.getJSONArray("Administrative_Penalty");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Administrative_Penalty"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					Map<String,byte[]> hashmap=new HashMap<String,byte[]>();
					for(Result r:resultbu){
						String penalty_code=Bytes.toString(r.getValue(Bytes.toBytes("Administrative_Penalty"), Bytes.toBytes("penalty_code")));
						hashmap.put(penalty_code,r.getRow());
						
					}
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes((String)t.get("rowkey")));
						String penalty_code=(String)t.get("Administrative_Penalty:penalty_code");
						Iterator it=t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						if(!hashmap.containsKey(penalty_code)){
							addbulist.add(p);
						}
					}
					//dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			
			/****
			 * 经营异常  先删除这个公司所有的经营异常，在添加
			 */
			if(jsonobj.containsKey("Business_Abnormal")){
				JSONArray business=jsonobj.getJSONArray("Business_Abnormal");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Business_Abnormal"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
									Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						addbulist.add(p);
					}
					for(Result r:resultbu){
						Delete delete=new Delete(r.getRow());
						deletebulist.add(delete);
					}
					dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/***
			 * 抽查检查信息  先删除这个公司全部抽查检查，在添加
			 */
			if(jsonobj.containsKey("Spot_Check")){
				JSONArray business=jsonobj.getJSONArray("Spot_Check");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Spot_Check"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
									Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						addbulist.add(p);
					}
					for(Result r:resultbu){
						Delete delete=new Delete(r.getRow());
						deletebulist.add(delete);
					}
					dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/****
			 * 股东信息 
			 * 
			 */
			if(jsonobj.containsKey("Shareholder_Info")){
				JSONArray business=jsonobj.getJSONArray("Shareholder_Info");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Shareholder_Info"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					Map<String,byte[]> sharemap=new HashMap<String,byte[]>();
					Set<String> set=new HashSet<String>();
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
						searchShareholdername+=(String)t.get("Shareholder_Info:shareholder_name");
						searchShareholdername+=" ";
						String	shareholder_certificationtype=t.get("Shareholder_Info:shareholder_certificationtype")==null ? "":t.get("Shareholder_Info:shareholder_certificationtype").toString();
						String  actualpaid_capital=t.get("Shareholder_Info:actualpaid_capital")==null ? "":t.get("Shareholder_Info:actualpaid_capital").toString();
						listShare.add((String)t.get("Shareholder_Info:shareholder_name")+"|"+(String)t.get("Shareholder_Info:shareholder_type")+"|"+shareholder_certificationtype+"|"+actualpaid_capital+"|"+"");
						Iterator it= t.keySet().iterator();
						set.add((String)t.get("Shareholder_Info:shareholder_name")+"_"+(String)t.get("Shareholder_Info:shareholder_type"));
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						addbulist.add(p);
					}
					int resultsize=0;
					for(Result r:resultbu){
						String shareholder_name=Bytes.toString(r.getValue(Bytes.toBytes("Shareholder_Info"), Bytes.toBytes("shareholder_name")));
						String shareholder_type=Bytes.toString(r.getValue(Bytes.toBytes("Shareholder_Info"), Bytes.toBytes("shareholder_type")));
						String certificationtype=Bytes.toString(r.getValue(Bytes.toBytes("Shareholder_Info"), Bytes.toBytes("shareholder_certificationtype")));
						String capital=Bytes.toString(r.getValue(Bytes.toBytes("Shareholder_Info"), Bytes.toBytes("actualpaid_capital")));
						String fundto=Bytes.toString(r.getValue(Bytes.toBytes("Shareholder_Info"), Bytes.toBytes("fundedratio")));
						sharemap.put(shareholder_name+"_"+shareholder_type, r.getRow());
                        oldListShare.add(shareholder_name+"|"+shareholder_type+"|"+certificationtype+"|"+capital+"|"+fundto);
                        resultsize++;
					}
					if(business.size()==resultsize){
						if(!sharemap.keySet().containsAll(set)){
							for (byte[] value : sharemap.values()) {   
								Delete delete=new Delete(value);
								deletebulist.add(delete);
							}  
							dao.commDeleteMethods(tableName, deletebulist);
							dao.commPutMethods(tableName, addbulist);
						}else{
							listShare.clear();
							listShare.addAll(oldListShare);
						}
					}else{
						for (byte[] value : sharemap.values()) {   
							Delete delete=new Delete(value);
							deletebulist.add(delete);
						}  
						dao.commDeleteMethods(tableName, deletebulist);
						dao.commPutMethods(tableName, addbulist);
					}
					
				}
			}
			/****
			 * 主要人员 先删除后添加
			 */
			if(jsonobj.containsKey("KeyPerson_Info")){
				JSONArray business=jsonobj.getJSONArray("KeyPerson_Info");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("KeyPerson_Info"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					Map<String,Put> personmap=new HashMap<String,Put>();
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
						searchKeyname+=(String)t.get("KeyPerson_Info:keyperson_name");
						searchKeyname+=" ";
						listPerson.add((String)t.get("KeyPerson_Info:keyperson_name")+"|"+(String)t.get("KeyPerson_Info:keyperson_position"));
						Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						addbulist.add(p);
						personmap.put((String)t.get("KeyPerson_Info:keyperson_name")+"_"+(String)t.get("KeyPerson_Info:keyperson_position"), p);
					}
					for(Result r:resultbu){
						String rnam=Bytes.toString(r.getValue(Bytes.toBytes("KeyPerson_Info"), Bytes.toBytes("keyperson_name")));
						String rposition=Bytes.toString(r.getValue(Bytes.toBytes("KeyPerson_Info"), Bytes.toBytes("keyperson_position")));
						if(!personmap.containsKey(rnam+"_"+rposition)){
							Delete delete=new Delete(r.getRow());
							deletebulist.add(delete);
						}else{
							addbulist.remove(personmap.get(rnam+"_"+rposition));
						}
					}
					dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/***
			 * 分支机构 先删除后添加
			 */
			if(jsonobj.containsKey("Branches")){
				JSONArray business=jsonobj.getJSONArray("Branches");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Branches"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes((String)t.get("rowkey")));
						Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						addbulist.add(p);
					}
					for(Result r:resultbu){
						Delete delete=new Delete(r.getRow());
						deletebulist.add(delete);
					}
					dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/***
			 * 清算信息表
			 */
			if(jsonobj.containsKey("liquidation_Information")){
				JSONArray business=jsonobj.getJSONArray("liquidation_Information");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("liquidation_Information"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						if(t.containsKey("rowkey")){
							Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
										Iterator it= t.keySet().iterator();
							while(it.hasNext()){
								String key=it.next().toString();
								String value=t.get(key)==null ? "":t.get(key).toString();
								if(!key.equals("rowkey")){
									p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
								}
							}
							addbulist.add(p);
						}
					}
					for(Result r:resultbu){
						Delete delete=new Delete(r.getRow());
						deletebulist.add(delete);
					}
					dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/*****
			 * 变更信息
			 */
			if(jsonobj.containsKey("Changed_Announcement")){
				JSONArray business=jsonobj.getJSONArray("Changed_Announcement");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Changed_Announcement"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					Map<String,byte[]> hashmap=new HashMap<String,byte[]>();
					for(Result r:resultbu){
						String event=Bytes.toString(r.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_events")));
						String date=Bytes.toString(r.getValue(Bytes.toBytes("Changed_Announcement"), Bytes.toBytes("changedannouncement_date")));
						hashmap.put(event+"_"+date,r.getRow());
					}
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes((String)t.get("rowkey")));
						String date="";
						String event=t.get("Changed_Announcement:changedannouncement_events")==null ? "":t.get("Changed_Announcement:changedannouncement_events").toString();
						if(t.containsKey("Changed_Announcement:changedannouncement_date")){
							 date=datautil.evaluate((String)t.get("Changed_Announcement:changedannouncement_date"));
						}
						Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
									if(!key.equals("Changed_Announcement:changedannouncement_date")){
									      p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
									}else{
									      p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(datautil.evaluate(value)));

									}
							}
						}
						if(!hashmap.containsKey(event+"_"+date)){
							addbulist.add(p);
						}
						
					}
					dao.commPutMethods(tableName, addbulist);
				}
			}
			/*****
			 * 股权出资  通过股权出资编号
			 */
			if(jsonobj.containsKey("Equity_Pledge")){
				JSONArray business=jsonobj.getJSONArray("Equity_Pledge");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Equity_Pledge"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					Map<String,byte[]> hashmap=new HashMap<String,byte[]>();
					for(Result r:resultbu){
						String equitypledgeno=Bytes.toString(r.getValue(Bytes.toBytes("Equity_Pledge"), Bytes.toBytes("equitypledge_no")));
						hashmap.put(equitypledgeno,r.getRow());	
					}
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes((String)t.get("rowkey")));
						String equitypledgeno="";
						if(t.containsKey("Equity_Pledge:equitypledge_no")){
							equitypledgeno=t.get("Equity_Pledge:equitypledge_no").toString();
						}
						Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						if(!hashmap.containsKey(equitypledgeno)){
							addbulist.add(p);
						}
						
					}
					//dao.commDeleteMethods(tableName, deletebulist);
					dao.commPutMethods(tableName, addbulist);
				}
			}
			
			/*****
			 * 动产抵押 是通过动产抵押编号判断
			 */
			if(jsonobj.containsKey("Chattel_Mortgage")){
				JSONArray business=jsonobj.getJSONArray("Chattel_Mortgage");
                if(business.size()>0){
					List<Put> addbulist=new ArrayList<Put>();
					List<Delete> deletebulist=new ArrayList<Delete>();
					Scan scan=new Scan();
					scan.addFamily(Bytes.toBytes("Chattel_Mortgage"));
					scan.setStartRow(Bytes.toBytes(companyName+"_01"));
					scan.setStopRow(Bytes.toBytes(companyName+"_99"));
					ResultScanner resultbu=dao.commScanMethods(tableName,scan);
					Map<String,byte[]> hashmap=new HashMap<String,byte[]>();
					for(Result r:resultbu){
						String chattelmortgage_registrationno=Bytes.toString(r.getValue(Bytes.toBytes("Chattel_Mortgage"), Bytes.toBytes("chattelmortgage_registrationno")));
						hashmap.put(chattelmortgage_registrationno,r.getRow());
						
					}
					for(int i=0;i<business.size();i++){
						JSONObject t = business.getJSONObject(i);
						Put p=new Put(Bytes.toBytes(t.get("rowkey").toString()));
						String chattelmortgage_registrationno=(String)t.get("Chattel_Mortgage:chattelmortgage_registrationno");
									Iterator it= t.keySet().iterator();
						while(it.hasNext()){
							String key=it.next().toString();
							String value=t.get(key)==null ? "":t.get(key).toString();
							if(!key.equals("rowkey")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}
						}
						if(!hashmap.containsKey(chattelmortgage_registrationno)){
							addbulist.add(p);
						}
						
					}
					dao.commPutMethods(tableName, addbulist);
				}
			}
			if(jsonobj.containsKey("Registered_Info")){
				JSONArray jsonarray=jsonobj.getJSONArray("Registered_Info");
				List<Put> addlist=new ArrayList<Put>();
				List<Delete> deletelist=new ArrayList<Delete>();
				Scan scan=new Scan();
				String rowkey="";
				String businessscope="";
				String registrationstatus="";
				String establishmentdate="";
				String legalrepresentative="";
				String residenceaddress="";
				String registrationno="";
				String zczb="";
				String entstatus=""; 
				scan.addFamily(Bytes.toBytes("Registered_Info"));
				scan.setStartRow(Bytes.toBytes(companyName+"_01"));
				scan.setStopRow(Bytes.toBytes(companyName+"_99"));
				ResultScanner result=dao.commScanMethods(tableName,scan);
				JSONObject  t= jsonarray.getJSONObject(0);
					if(t.containsKey("Registered_Info:registeredcapital")){
						 Pattern p=Pattern.compile("(\\d+)"); 
						 String registeredcapital=t.get("Registered_Info:registeredcapital").toString().replace(",", "");
						  Matcher m=p.matcher(registeredcapital);
						  String zb="";
						  if(m.find()){
							  zb=m.group(1);
						  } 
						    String regex="([\\u4e00-\\u9fa5]+)";
					    	Matcher matcher = Pattern.compile(regex).matcher(registeredcapital);
					    	if(matcher.find()){
								  zczb= parsemoney.evaluate(zb,matcher.group(1)); 
					    	}
					}
					rowkey=(String)t.get("rowkey");
					if(t.containsKey("Registered_Info:businessscope")){
						businessscope=t.get("Registered_Info:businessscope")==null ? "":t.get("Registered_Info:businessscope").toString();
					}
					if(t.containsKey("Registered_Info:registrationstatus")){
						registrationstatus=t.get("Registered_Info:registrationstatus")==null ? "":t.get("Registered_Info:registrationstatus").toString();
					}
					if(t.containsKey("Registered_Info:establishmentdate")){
						establishmentdate=datautil.evaluate((String)t.get("Registered_Info:establishmentdate")).equals("") ? "" : datautil.evaluate((String)t.get("Registered_Info:establishmentdate"))+"T09:05:53.065Z";
					}
					if(t.containsKey("Registered_Info:legalrepresentative")){
						legalrepresentative=t.get("Registered_Info:legalrepresentative")==null ? "":t.get("Registered_Info:legalrepresentative").toString();
					}
					if(t.containsKey("Registered_Info:residenceaddress")){
					  	residenceaddress=t.get("Registered_Info:residenceaddress")==null ? "":t.get("Registered_Info:residenceaddress").toString();
					}
					if(t.containsKey("Registered_Info:registrationno")){
						registrationno=(String)t.get("Registered_Info:registrationno");

					}
					if(t.containsKey("Registered_Info:entstatus")){
						entstatus=t.get("Registered_Info:entstatus")==null ? "":t.get("Registered_Info:entstatus").toString();
					}
					Put p=new Put(Bytes.toBytes(rowkey));
					Iterator it= t.keySet().iterator();
					while(it.hasNext()){
						String key=it.next().toString();
						String value=t.get(key)==null ? "":t.get(key).toString();
						if(!key.equals("rowkey") && !key.equals("Registered_Info:registeredcapital")){
							if(!key.equals("Registered_Info:establishmentdate")){
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(value));
							}else{
								p.add(Bytes.toBytes(key.split(":")[0]),Bytes.toBytes(key.split(":")[1]), Bytes.toBytes(datautil.evaluate(value)));
							}
						}
					}
					p.add(Bytes.toBytes("Registered_Info"),Bytes.toBytes("registeredcapital"), Bytes.toBytes(zczb));
					if(t.containsKey("Registered_Info:registeredcapital")){
						p.add(Bytes.toBytes("Registered_Info"),Bytes.toBytes("orgregisteredcapital"), Bytes.toBytes(t.get("Registered_Info:registeredcapital")+""));
					}else{
						p.add(Bytes.toBytes("Registered_Info"),Bytes.toBytes("orgregisteredcapital"), Bytes.toBytes(""));
					}
				int index=0;
				for(Result r:result){
					 
					 if(Bytes.toString(r.getRow()).equals(rowkey)){
							dao.commPutMethods(tableName, addlist);
					 }else{
						 for(Cell c:r.rawCells()){
							if(!t.keySet().contains(Bytes.toString(c.getQualifier()))){
								p.add(Bytes.toBytes("Registered_Info"), c.getQualifier(), c.getValue());
							}
						 }
						 dao.commPutMethods(tableName, p);
					 }
					 index++;
				
				}
				if(index==0){
					dao.commPutMethods(tableName, p);
				}
				
				//dao.commDeleteMethods(tableName, deletelist);
				
				/****
				 * 处理搜索问题
				 */
				Scan scansearch=new Scan();
				scansearch.addFamily(Bytes.toBytes("keyword"));
				scansearch.setStartRow(Bytes.toBytes(companyName+"_01"));
				scansearch.setStopRow(Bytes.toBytes(companyName+"_02"));
				ResultScanner resultSearch=dao.commScanMethods("search",scansearch);
				String serachkey="";
				for(Result r:resultSearch){
					serachkey=Bytes.toString(r.getRow());
				}
				Put psearch=new Put(Bytes.toBytes(rowkey));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("businessscope"), Bytes.toBytes(businessscope));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("registrationstatus"), Bytes.toBytes(registrationstatus));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("establishmentdate"), Bytes.toBytes(establishmentdate));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("keypersonname"), Bytes.toBytes(searchKeyname.trim()));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("legalrepresentative"), Bytes.toBytes(legalrepresentative));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("residenceaddress"), Bytes.toBytes(residenceaddress));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("shareholdername"), Bytes.toBytes(searchShareholdername.trim()));
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("enterprisename"), Bytes.toBytes(companyName));
				if(zczb.equals("")){
					zczb="-100";
				}
				psearch.add(Bytes.toBytes("keyword"), Bytes.toBytes("registeredcapital"), Bytes.toBytes(zczb));
				if(!serachkey.equals("")){
				 dao.commDeleteMethods("search", new Delete(Bytes.toBytes(serachkey)));
				}
				dao.commPutMethods("search", psearch);
				
				/****
				 * 处理Ne4J代码
				 * 
				 */
				Map<String,Object> ne4jmap=new HashMap<String,Object>();
				NeoHelper ne=new NeoHelper();
				UpdateNeo4j updateNeo4j=new UpdateNeo4j();
				ne4jmap.put("companyName", companyName);
				ne4jmap.put("registrationno",registrationno);
				ne4jmap.put("establishmentdate", establishmentdate);
				ne4jmap.put("registeredcapital",zczb);
				ne4jmap.put("entstatus", entstatus);
				ne4jmap.put("frname", legalrepresentative);
				ne4jmap.put("personList", listPerson);
				ne4jmap.put("listShare", listShare);
				ne.pudateEntname(ne4jmap, updateNeo4j);
				ne.pudateStepRegistration(ne4jmap, updateNeo4j);
				ne.pudatePosition(ne4jmap, updateNeo4j);
				ne.pudateShare(ne4jmap, updateNeo4j);
				updateNeo4j.commit();
			}
	       Put p=new Put(Bytes.toBytes(companyName));
	       p.add(Bytes.toBytes("LastUpdateTime"),Bytes.toBytes("crawler_gs"), Bytes.toBytes(new SimpleDateFormat("yyyy-MM-dd").format(new Date())));
	       dao.commPutMethods("LengJingThirdPartInterfaceRecordTemp2", p);
	       
	       SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
	       Log.info("companyname:"+":"+sdf.format(new Date())+":"+companyName);
	       /****
	        * 更新成功后，进行回调数据
	        */
	        //现网环境
			/*String url="http://ljzd3.lengjing.info/updateStatusFeedback?companyName="+
						URLEncoder.encode(companyName, "GBK")+"&progress=1&type=GS";
			HttpConnectHelper hih=new HttpConnectHelper();
			hih.feedback(url);
			//试用环境
			String urlSHIYONG="http://172.16.0.100:8080/lengjing/updateStatusFeedback?companyName="+
						URLEncoder.encode(companyName, "GBK")+"&progress=1&type=GS";
			hih.feedback(urlSHIYONG);*/
			//测试环境
			String urltest="http://172.16.0.101:8080/lengjing/updateStatusFeedback?companyName="+
					URLEncoder.encode(companyName, "GBK")+"&progress=1&type=GS";
			connect.feedback(urltest);
			

		} catch (Exception e) {
			e.printStackTrace();
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");			
			Log.info("errorcompanyname:"+sdf.format(new Date())+":"+companyName);
			//测试环境
			String urltest="http://172.16.0.101:8080/lengjing/updateStatusFeedback?companyName="+
					URLEncoder.encode(companyName, "GBK")+"&progress=false&type=GS";
			connect.feedback(urltest);
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		String json="{\"inputCompanyName\": \"上海明蕾商贸有限公司\", \"KeyPerson_Info\": [{\"KeyPerson_Info:keyperson_position\": \"执行董事\", \"KeyPerson_Info:registrationno\": \"3101142099463\", \"KeyPerson_Info:keyperson_name\": \"王建明\", \"KeyPerson_Info:keyperson_no\": \"1\", \"KeyPerson_Info:id\": \"1\", \"KeyPerson_Info:enterprisename\": \"上海明蕾商贸有限公司\", \"rowkey\": \"上海明蕾商贸有限公司_06_3101142099463_201609021\"}, {\"KeyPerson_Info:keyperson_position\": \"执行董事\", \"KeyPerson_Info:registrationno\": \"3101142099463\", \"KeyPerson_Info:keyperson_name\": \"王建明\", \"KeyPerson_Info:keyperson_no\": \"2\", \"KeyPerson_Info:id\": \"2\", \"KeyPerson_Info:enterprisename\": \"上海明蕾商贸有限公司\", \"rowkey\": \"上海明蕾商贸有限公司_06_3101142099463_201609022\"}, {\"KeyPerson_Info:keyperson_position\": \"监事\", \"KeyPerson_Info:registrationno\": \"3101142099463\", \"KeyPerson_Info:keyperson_name\": \"赵静\", \"KeyPerson_Info:keyperson_no\": \"3\", \"KeyPerson_Info:id\": \"3\", \"KeyPerson_Info:enterprisename\": \"上海明蕾商贸有限公司\", \"rowkey\": \"上海明蕾商贸有限公司_06_3101142099463_201609023\"}], \"Registered_Info\": [{\"Registered_Info:registrationno\": \"3101142099463\", \"Registered_Info:registeredcapital\": \"50.000000万人民币\", \"Registered_Info:validityfrom\": \"2005年3月10日\", \"Registered_Info:legalrepresentative\": \"王建明\", \"Registered_Info:dxrq\": \"2007年4月5日\", \"Registered_Info:approvaldate\": \"2005年3月10日\", \"Registered_Info:residenceaddress\": \"上海沪宜公路1158号\", \"Registered_Info:validityto\": \"2015年3月9日\", \"Registered_Info:establishmentdate\": \"2005年3月10日\", \"Registered_Info:province\": \"上海市\", \"rowkey\": \"上海明蕾商贸有限公司_01_3101142099463_\", \"Registered_Info:enterprisetype\": \"有限责任公司\", \"Registered_Info:registrationinstitution\": \"嘉定区市场监管局\", \"Registered_Info:businessscope\": \"化妆品、家居护理用品、日用百货、针纺织品、日用洗涤用品、建筑防水材料、工艺品、床上用品、服装鞋帽的销售。（涉及行政许可的，凭许可证经营）。\", \"Registered_Info:registrationstatus\": \"吊销，未注销\", \"Registered_Info:enterprisename\": \"上海明蕾商贸有限公司\"}], \"Shareholder_Info\": [{\"Shareholder_Info:id\": \"1\", \"Shareholder_Info:registrationno\": \"3101142099463\", \"Shareholder_Info:shareholder_name\": \"王建明\", \"rowkey\": \"上海明蕾商贸有限公司_04_3101142099463_201609021\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:shareholder_certificationno\": \"\", \"Shareholder_Info:enterprisename\": \"上海明蕾商贸有限公司\", \"Shareholder_Info:shareholder_details\": \"https://www.sgs.gov.cn/notice/notice/view_investor?uuid=zBXHaUcvIN5f.aBhv0IrOA==\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}, {\"Shareholder_Info:id\": \"2\", \"Shareholder_Info:registrationno\": \"3101142099463\", \"Shareholder_Info:shareholder_name\": \"赵静\", \"rowkey\": \"上海明蕾商贸有限公司_04_3101142099463_201609022\", \"Shareholder_Info:shareholder_certificationtype\": \"中华人民共和国居民身份证\", \"Shareholder_Info:shareholder_certificationno\": \"\", \"Shareholder_Info:enterprisename\": \"上海明蕾商贸有限公司\", \"Shareholder_Info:shareholder_details\": \"https://www.sgs.gov.cn/notice/notice/view_investor?uuid=W86iLzBXUlVf.aBhv0IrOA==\", \"Shareholder_Info:shareholder_type\": \"自然人股东\"}], \"taskId\": \"null\", \"accountId\": \"null\"}";
		loadData(json);
		
	}

}
