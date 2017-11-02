package com.lengjing.consumer;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;


public class XmlParse {
	
	public static void parse(String xml,String personName,String companyname,String idCard) throws DocumentException, IOException{
		Document doc = null;
		Date dNow = new Date();
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
		doc = (Document) DocumentHelper.parseText(xml); // 将字符串转为XML
		Element rootElt = doc.getRootElement(); // 获取根节点
		HbaseDaoImpl dao=new HbaseDaoImpl();
		List<Put> putlist=new ArrayList<Put>();
		List<Put> putlistDetail=new ArrayList<Put>();
		List<Put> putlistlog=new ArrayList<Put>();
		Iterator iter = rootElt.elementIterator("RYPOSFR"); // 获取根节点下的子节点RYPOSFR （企業法人信息）
		String uuid="";
		String newuuid=UUID.randomUUID().toString();
		int index=1;
		// 遍历BASIC节点
		while (iter.hasNext()) {
			Element recordEle = (Element) iter.next();
			Iterator itersItem = recordEle.elementIterator("ITEM"); // 获取子节点BASIC下的子节点ITEM	
			while (itersItem.hasNext()) {
				  Element itemEle = (Element) itersItem.next();
				  if(personName.equals(itemEle.elementTextTrim("RYNAME"))){
					  String ryname = itemEle.elementTextTrim("RYNAME");
					  String entname = itemEle.elementTextTrim("ENTNAME");
					  String regno = itemEle.elementTextTrim("REGNO");
					  String enttype = itemEle.elementTextTrim("ENTTYPE");
					  String regcap = itemEle.elementTextTrim("REGCAP");
					  String regcapcur = itemEle.elementTextTrim("REGCAPCUR");
					  String entstatus = itemEle.elementTextTrim("ENTSTATUS");
					  if((uuid==null || uuid.equals(""))){
						  if(idCard==null && idCard.equals("")){
							     Scan scan=new Scan();
								 scan.setStartRow(Bytes.toBytes(ryname+"_"+idCard+"_1"));
								 scan.setStopRow(Bytes.toBytes(ryname+"_"+idCard+"_3"));
								 ResultScanner result= dao.commScanMethods("PersonQuery", scan);
								 for(Result r:result){
									 uuid=Bytes.toString(r.getValue(Bytes.toBytes("relation"), Bytes.toBytes("uuid")));
								 } 
						  }else{
							     Get get=new Get(Bytes.toBytes(personName+"_"+idCard));
								 Result r= dao.commGetMethods("LengJingSFPersonRelation",get);
								 if(!r.isEmpty()){
									 uuid=Bytes.toString(r.getValue(Bytes.toBytes("relation"), Bytes.toBytes("uuid")));
							    }   
						  }
					  }
					  
					  
					  /**PersonQuery表插入值***/
					  String rowkey=ryname+"_"+entname+"_2_法人";
					  Put put=new Put(Bytes.toBytes(rowkey));
					  if(uuid==null || uuid.equals("")){
						  uuid=newuuid;
					  }
					  put.add(Bytes.toBytes("relation"),Bytes.toBytes("uuid"),Bytes.toBytes(uuid));
					  putlist.add(put);
					  
					  /****
					   * 插入日志表
					   */
					    Put putlog=new Put(Bytes.toBytes(rowkey));
						putlog.add(Bytes.toBytes("LastUpdateTime"),Bytes.toBytes("zhongshu"),Bytes.toBytes(sdf.format(dNow)));
						putlistlog.add(putlog);
					
					  /**PersonQueryDetail表插入值***/
					  String rowkeyDetail=uuid+"_"+entname+"_"+index;
					  Put putDetail=new Put(Bytes.toBytes(rowkeyDetail));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("ryname"),Bytes.toBytes(ryname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("entname"),Bytes.toBytes(entname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regno"),Bytes.toBytes(regno));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("enttype"),Bytes.toBytes(enttype));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcap"),Bytes.toBytes(regcap));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcapcur"),Bytes.toBytes(regcapcur));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("entstatus"),Bytes.toBytes(entstatus));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("relationkey"),Bytes.toBytes(rowkey));
					  putlistDetail.add(putDetail);
				  }
				}
			}
		Iterator rypossha = rootElt.elementIterator("RYPOSSHA"); // 获取根节点下的子节点RYPOSFR (股東信息)
		// 遍历BASIC节点
		while (rypossha.hasNext()) {
			Element recordEle = (Element) rypossha.next();
			Iterator itersItem = recordEle.elementIterator("ITEM"); // 获取子节点BASIC下的子节点ITEM
			while (itersItem.hasNext()) {
				  Element itemEle = (Element) itersItem.next();
				  if(personName.equals(itemEle.elementTextTrim("RYNAME"))){
					  String ryname = itemEle.elementTextTrim("RYNAME");
					  String entname = itemEle.elementTextTrim("ENTNAME");
					  String regno = itemEle.elementTextTrim("REGNO");
					  String enttype = itemEle.elementTextTrim("ENTTYPE");
					  String regcap = itemEle.elementTextTrim("REGCAP");
					  String regcapcur = itemEle.elementTextTrim("REGCAPCUR");
					  String subconam = itemEle.elementTextTrim("SUBCONAM");
					  String currency = itemEle.elementTextTrim("CURRENCY");
					  String conform = itemEle.elementTextTrim("CONFORM");
					  String fundedratio = itemEle.elementTextTrim("FUNDEDRATIO");
					  String entstatus = itemEle.elementTextTrim("ENTSTATUS");
					  if((uuid==null || uuid.equals(""))){
						  if(idCard==null && idCard.equals("")){
							     Scan scan=new Scan();
								 scan.setStartRow(Bytes.toBytes(ryname+"_"+idCard+"_1"));
								 scan.setStopRow(Bytes.toBytes(ryname+"_"+idCard+"_3"));
								 ResultScanner result= dao.commScanMethods("PersonQuery", scan);
								 for(Result r:result){
									 uuid=Bytes.toString(r.getValue(Bytes.toBytes("relation"), Bytes.toBytes("uuid")));
								 } 
						  }else{
							     Get get=new Get(Bytes.toBytes(personName+"_"+idCard));
								 Result r= dao.commGetMethods("LengJingSFPersonRelation",get);
								 if(!r.isEmpty()){
									 uuid=Bytes.toString(r.getValue(Bytes.toBytes("relation"), Bytes.toBytes("uuid")));
							    }   
						  }
					  }
					  if(uuid==null || uuid.equals("")){
						  uuid=newuuid;
					  }
	 				  String rowkey=ryname+"_"+entname+"_1_股东";
	 				  Put put=new Put(Bytes.toBytes(rowkey));
	 				  put.add(Bytes.toBytes("relation"),Bytes.toBytes("uuid"),Bytes.toBytes(uuid));
	 				 putlist.add(put);
	 				 
	 				  /****
					   * 插入日志表
					   */
					    Put putlog=new Put(Bytes.toBytes(rowkey));
						putlog.add(Bytes.toBytes("LastUpdateTime"),Bytes.toBytes("zhongshu"),Bytes.toBytes(sdf.format(dNow)));
						putlistlog.add(putlog);
	 				
	 				 /**PersonQueryDetail表插入值***/
					  String rowkeyDetail=uuid+"_"+entname+"_"+index;
					  Put putDetail=new Put(Bytes.toBytes(rowkeyDetail));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("ryname"),Bytes.toBytes(ryname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("entname"),Bytes.toBytes(entname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regno"),Bytes.toBytes(regno));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("enttype"),Bytes.toBytes(enttype));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcap"),Bytes.toBytes(regcap));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcapcur"),Bytes.toBytes(regcapcur));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("subconam"),Bytes.toBytes(subconam));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("currency"),Bytes.toBytes(currency));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("conform"),Bytes.toBytes(conform));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("fundedratio"),Bytes.toBytes(fundedratio));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("entstatus"),Bytes.toBytes(entstatus));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("relationkey"),Bytes.toBytes(rowkey));
					  putlistDetail.add(putDetail);
					  index++;
	
					}
			   }
			}
		Iterator ryposper = rootElt.elementIterator("RYPOSPER"); // 获取根节点下的子节点RYPOSPER（任職信息）
		// 遍历BASIC节点
		while (ryposper.hasNext()) {
			Element recordEle = (Element) ryposper.next();
			Iterator itersItem = recordEle.elementIterator("ITEM"); // 获取子节点BASIC下的子节点ITEM
			while (itersItem.hasNext()) {
				  Element itemEle = (Element) itersItem.next();
				  if(personName.equals(itemEle.elementTextTrim("RYNAME"))){
					  String ryname = itemEle.elementTextTrim("RYNAME");
					  String entname = itemEle.elementTextTrim("ENTNAME");
					  String regno = itemEle.elementTextTrim("REGNO");
					  String enttype = itemEle.elementTextTrim("ENTTYPE");
					  String regcap = itemEle.elementTextTrim("REGCAP");
					  String regcapcur = itemEle.elementTextTrim("REGCAPCUR");
					  String position = itemEle.elementTextTrim("POSITION");
					  String rowkey=ryname+"_"+entname+"_2_"+position;
					  Put put=new Put(Bytes.toBytes(rowkey));
					  put.add(Bytes.toBytes("relation"),Bytes.toBytes("uuid"),Bytes.toBytes(uuid));
					  putlist.add(put);
					  
					  /****
					   * 插入日志表
					   */
					    Put putlog=new Put(Bytes.toBytes(rowkey));
						putlog.add(Bytes.toBytes("LastUpdateTime"),Bytes.toBytes("zhongshu"),Bytes.toBytes(sdf.format(dNow)));
						putlistlog.add(putlog);
					 
					  /**PersonQueryDetail表插入值***/
					  String rowkeyDetail=uuid+"_"+entname+"_"+index;
					  Put putDetail=new Put(Bytes.toBytes(rowkeyDetail));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("ryname"),Bytes.toBytes(ryname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("entname"),Bytes.toBytes(entname));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regno"),Bytes.toBytes(regno));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("enttype"),Bytes.toBytes(enttype));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcap"),Bytes.toBytes(regcap));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("regcapcur"),Bytes.toBytes(regcapcur));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("position"),Bytes.toBytes(position));
					  putDetail.add(Bytes.toBytes("detail"),Bytes.toBytes("relationkey"),Bytes.toBytes(rowkey));
					  putlistDetail.add(putDetail);
					  index++;
				  }
				}
			}
		/***
		 * 插入hbase表
		 */
		 Scan scan=new Scan();
		 Filter rowfilter=new RowFilter(CompareOp.EQUAL, new RegexStringComparator(uuid));
		 scan.setFilter(rowfilter);
		 ResultScanner result= dao.commScanMethods("PersonQueryDetail", scan);
		 for(Result r:result){
				 dao.commDeleteMethods("PersonQuery", new Delete(r.getValue(Bytes.toBytes("detail"), Bytes.toBytes("relationkey"))));
				 dao.commDeleteMethods("PersonQueryDetail", new Delete(r.getRow()));
			 
		 }
		 //插入数据
		if(idCard!=null && !idCard.equals("")){
			Put p=new Put(Bytes.toBytes(personName+"_"+idCard));
			p.add(Bytes.toBytes("relation"),Bytes.toBytes("uuid"),Bytes.toBytes(uuid));
			putlist.add(p);
			
			/****
			 * 插入人对应关系表
			 */
			Put personRelation=new Put(Bytes.toBytes(personName+"_"+idCard));
			personRelation.add(Bytes.toBytes("relation"),Bytes.toBytes("uuid"),Bytes.toBytes(uuid));
			dao.commPutMethods("LengJingSFPersonRelation", personRelation);
		}
		dao.commPutMethods("PersonQuery", putlist);
		dao.commPutMethods("PersonQueryDetail", putlistDetail);
		dao.commPutMethods("LengJingThirdPartInterfaceRecordTemp", putlistlog);
		
		System.out.println("DDD");
	}
	
	
	
	public static void main(String[] args) throws DocumentException, IOException {
		String xml="<?xml version='1.0' encoding='UTF-8'?><DATA><ORDERLIST><ITEM><UID>F12A149BC9ECA4C9</UID><ORDERNO>160808172807571034776</ORDERNO><KEY>330106196710010164</KEY><KEYTYPE>9</KEYTYPE><STATUS>1</STATUS><FINISHTIME>2016-08-08 17:28:08</FINISHTIME></ITEM></ORDERLIST><RYPOSFR></RYPOSFR><RYPOSSHA><ITEM><RYNAME>张英</RYNAME><ENTNAME>杭州酩乐企业管理咨询有限公司</ENTNAME><REGNO>330194000006487</REGNO><ENTTYPE>私营有限责任公司（自然人控股或私营性质企业控股）</ENTTYPE><REGCAP>475.000000</REGCAP><REGCAPCUR>人民币</REGCAPCUR><SUBCONAM></SUBCONAM><CURRENCY></CURRENCY><CONFORM></CONFORM><FUNDEDRATIO>〈0.01%</FUNDEDRATIO><ENTSTATUS>在营（开业）</ENTSTATUS></ITEM><ITEM><RYNAME>张英</RYNAME><ENTNAME>上海云锋新创股权投资中心（有限合伙）</ENTNAME><REGNO>310000000127359</REGNO><ENTTYPE>有限合伙企业</ENTTYPE><REGCAP>10000.000000</REGCAP><REGCAPCUR>人民币</REGCAPCUR><SUBCONAM></SUBCONAM><CURRENCY></CURRENCY><CONFORM></CONFORM><FUNDEDRATIO>〈0.01%</FUNDEDRATIO><ENTSTATUS>在营（开业）</ENTSTATUS></ITEM><ITEM><RYNAME>张英</RYNAME><ENTNAME>杭州坤宝投资咨询有限公司</ENTNAME><REGNO>330106000053332</REGNO><ENTTYPE>私营有限责任公司（自然人控股或私营性质企业控股）</ENTTYPE><REGCAP>2500.000000</REGCAP><REGCAPCUR>人民币</REGCAPCUR><SUBCONAM></SUBCONAM><CURRENCY></CURRENCY><CONFORM></CONFORM><FUNDEDRATIO>〈0.01%</FUNDEDRATIO><ENTSTATUS>在营（开业）</ENTSTATUS></ITEM></RYPOSSHA><RYPOSPER><ITEM><RYNAME>张英</RYNAME><ENTNAME>杭州坤宝投资咨询有限公司</ENTNAME><REGNO>330106000053332</REGNO><ENTTYPE>私营有限责任公司（自然人控股或私营性质企业控股）</ENTTYPE><REGCAP>2500.000000</REGCAP><REGCAPCUR>人民币</REGCAPCUR><ENTSTATUS>在营（开业）</ENTSTATUS><POSITION>董事兼总经理</POSITION></ITEM></RYPOSPER><PERSONCASEINFO></PERSONCASEINFO></DATA>";
		XmlParse parse = new XmlParse();
		//parse.parse(xml,name);
		System.out.println("DD");
	}

}
