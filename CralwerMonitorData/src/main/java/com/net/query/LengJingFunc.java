package com.net.query;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 1、根据页面上所有的公司节点，获取与其关联的所有自然人
 * 2、找出所有的重名自然人节点id，通过计算其相互之间的最短距离进行递归合并
 * 3、根据同时任职关系进行进一步合并
 */
public class LengJingFunc {

	/**
	 * 本轮计算涉及到的与自然人有关的关系
	 */
	JSONArray relationshipArr=new JSONArray();
	
	/**
	 * 已计算过的自然人id集合，在此名单中的节点彼此间不需要重新计算
	 */
	JSONArray personIdArr1=new JSONArray();
	/**
	 * 本轮待计算的自然人id集合
	 */
	JSONArray personIdArr2=new JSONArray();
	/**
	 * 本轮待计算的自然人姓名集合
	 */
	HashSet<String> waitForNamePersonNameSet=new HashSet<String>();
	/**
	 * key：合并前自然人id，val：合并后自然人id
	 */
	HashMap<String,String> old2New=new HashMap<String,String>();
	/**
	 *  key：自然人姓名，val：关联公司集合
	 */
	Map<String,TreeSet<String>> personName2CompanyIdSet=new TreeMap<String,TreeSet<String>>();
	/**
	 * key：自然人id，val：自然人姓名
	 */
	HashMap<String,String> personId2Name=new HashMap<String,String>();
	/**
	 * key：公司id，val：公司名称
	 */
	HashMap<String,String> companyId2Name=new HashMap<String,String>();
	/**
	 * key：公司组合，val：同时关联的自然人姓名列表
	 */
	HashMap<String,TreeSet<String>> couple2PersonNameSet=new HashMap<String,TreeSet<String>>();
	/**
	 * key：公司id+'|'+自然人姓名，val：自然人id
	 */
	HashMap<String,String> companyIdPersonName2PersonId=new HashMap<String,String>();
	/**
	 * key：自然人id，val：groupId
	 */
	HashMap<String,Integer> groupIdMap=new HashMap<String,Integer>();
	/**
	 * 合并结果保存列表，可以被合并的节点会被放到一个set中
	 */
	ArrayList<TreeSet<String>> groupList=new ArrayList<TreeSet<String>>();
	/**
	 * key:自然人节点id, val:关联的公司名称
	 */
	JSONObject personId2CompanyName=new JSONObject();
	/**
	 * 需要进行合并计算的公司节点id集合
	 */
	HashSet<String> waitForMergeCompanyId=new HashSet<String>();
	/**
	 * 已经进行过合并计算的公司节点id集合，此部分公司不需再次进行计算
	 */
//	HashSet<String> waitForMergeCompanyIdBlacklist=new HashSet<String>();
	
	/**
	 * 页面上已经被删除掉的公司节点
	 */
	HashSet<String> deleteCompanyIdSet=new HashSet<String>();
	
	/**
	 * 棱镜一下功能构造方法
	 * @param companyNodeIdArrText 当前页面上所有的公司节点id，JSONArray
	 */
	public LengJingFunc(String companyNodeIdArrText)
	{
//		allCompanyNodeIdStr=companyNodeIds;
		JSONArray companyNodeArr=JSONArray.parseArray(companyNodeIdArrText);
		for(int i=0;i<companyNodeArr.size();i++)
		{
			String companyId=companyNodeArr.getString(i);
			waitForMergeCompanyId.add(companyId);
//			if(!waitForMergeCompanyIdBlacklist.contains(companyId))
//			{
//				waitForMergeCompanyId.add(companyId);
//			}
		}
	}
	
	/**
	 * 根据上一次棱镜一下的结果，初始化personIdArr1
	 * @param personIdArrText 上一次调用“棱镜一下”personIdArr的计算结果
	 */
	public void initPersonIdArr1(String personIdArrText)
	{
		if(personIdArrText.length()==0) return;
		JSONArray arr=JSONArray.parseArray(personIdArrText);
		personIdArr1.addAll(arr);
	}
	
	/**
	 * 获取本次personIdArr1的结果供下一次使用，输出为json格式，覆盖方式写入到页面上
	 * @return 
	 */
	public String getPersonIdArr1Text()
	{
		return personIdArr1.toJSONString();
	}
	
	/**
	 * 根据前一次合并计算结果，初始化old2New
	 * @param old2NewStr
	 */
	public void initOld2New(String old2NewText)
	{
		if(old2NewText.length()==0) return;
		JSONObject jsonObj=JSONObject.parseObject(old2NewText);
		Iterator<Entry<String, Object>> iterator = jsonObj.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<String, Object> entry = iterator.next();
			String oldId=entry.getKey();
			String newId=(String) entry.getValue();
			old2New.put(oldId,newId);
//			mergeNodes(oldId,newId);
		}
//		System.out.println("old2New:"+old2New);
//		System.out.println("groupList:"+groupList);
	}
	
	/**
	 * 获取当前old2New的结果供下一次使用，json格式，覆盖方式写入到页面上
	 * @return
	 */
	public String getOld2NewText()
	{
		JSONObject jsonObj=new JSONObject();
		jsonObj.putAll(old2New);
		return jsonObj.toJSONString();
	}
	
	
	/**
	 * 获取公司id+'|'+自然人id与自然人姓名的对应关系，json格式，覆盖方式写入到页面上
	 * @return
	 */
	public String getCompanyIdPersonName2PersonIdText()
	{
		JSONObject jsonObj=new JSONObject();
		jsonObj.putAll(companyIdPersonName2PersonId);
		return jsonObj.toJSONString();
	}
	
	
	/**
	 * 根据上一次调用棱镜一下的计算结果，初始化companyIdPersonName2PersonId
	 * @param companyIdPersonName2PersonIdText
	 */
	public void initCompanyIdPersonName2PersonId(String companyIdPersonName2PersonIdText)
	{
		if(companyIdPersonName2PersonIdText.length()==0) return;
		JSONObject jsonObj = JSONObject.parseObject(companyIdPersonName2PersonIdText);
		Iterator<Entry<String, Object>> iterator = jsonObj.entrySet().iterator();
		
		HashSet<String> tmpSet1=new HashSet<String>();		
		while(iterator.hasNext())
		{
			Entry<String, Object> entry = iterator.next();
			
			String companyIdPersonName=entry.getKey();
			String personId=(String) entry.getValue();
			
			String companyId=companyIdPersonName.split("\\|")[0];
			String personName=companyIdPersonName.split("\\|")[1];
			
			if(!personName2CompanyIdSet.containsKey(personName))
			{
				personName2CompanyIdSet.put(personName, new TreeSet<String>());
			}
			
			personName2CompanyIdSet.get(personName).add(companyId);
			
			deleteCompanyIdSet.add(companyId);
			tmpSet1.add(companyId);
			companyIdPersonName2PersonId.put(companyIdPersonName, personId);
			personId2Name.put(personId, personName);
		}
		
		deleteCompanyIdSet.removeAll(waitForMergeCompanyId);
		waitForMergeCompanyId.removeAll(tmpSet1);
	}
	
	/**
	 * 获取本轮计算涉及到的与自然人有关的关系，追加方式写入到页面上
	 * @return relationshipArr，json格式
	 */
	public String getRelationshipText()
	{
		return relationshipArr.toJSONString();
	}

	/**
	 * 将步长在3度以内或者有多人组合频繁出现的同名自然人合并到同一节点
	 * @param id1 自然人1节点id
	 * @param id2 自然人2节点id
	 */
	public void mergeNodes(String id1,String id2)
	{
		if(groupIdMap.containsKey(id1) && groupIdMap.containsKey(id2))
		{
			int groupId1=groupIdMap.get(id1);
			int groupId2=groupIdMap.get(id2);
			if(groupId1!=groupId2)
			{
				for(String id:groupList.get(groupId2))
				{
					groupList.get(groupId1).add(id);
					groupIdMap.put(id, groupId1);
				}
				groupList.set(groupId2,null);
			}
		}
		else if(groupIdMap.containsKey(id1) && !groupIdMap.containsKey(id2))
		{
			int groupId=groupIdMap.get(id1);
			groupList.get(groupId).add(id2);
			groupIdMap.put(id2, groupId);
		}
		else if(!groupIdMap.containsKey(id1) && groupIdMap.containsKey(id2))
		{
			int groupId=groupIdMap.get(id2);
			groupList.get(groupId).add(id1);
			groupIdMap.put(id1, groupId);
		}
		else
		{
			groupList.add(new TreeSet<String>());
			int groupId=groupList.size()-1;
			groupList.get(groupId).add(id1);
			groupList.get(groupId).add(id2);
			groupIdMap.put(id1, groupId);
			groupIdMap.put(id2, groupId);
		}
	}
	
	
	/**
	 * 全库查找，通过关系网对重复名字的节点进行合并计算
	 * @param personNames
	 * @throws Exception 
	 */
	
	public void mergePersonNames() throws Exception
	{
		
		if(personIdArr2.size()==0)
		{
			return;
		}
		
		HashSet<String> tmpSet=new HashSet<String>();
		for(int i=0;i<personIdArr1.size();i++)
		{
			String personId=personIdArr1.getString(i);
			String personName=personId2Name.get(personId);
			if(waitForNamePersonNameSet.contains(personName));
			{
				tmpSet.add(personId);
			}
		}
		
		String arr1=StringUtils.join(tmpSet,",");
		String arr2=StringUtils.join(personIdArr2,",");
		
//		System.out.println(personNames);
		String query=String.format("with [%s] as arr1,[%s] as arr2 "
				+ "match p=allShortestPaths((n1:Person)-[*1..3]-(n2:Person))  "
				+ "where id(n1) in arr1 and id(n2) in arr2 and n1.姓名=n2.姓名 and id(n1)>id(n2) "
				+ "return p "
				, arr1,arr2);
		String graphText= Neo4j.execute(query,"graph");
//		System.out.println(graphText);
		JSONObject root = JSONObject.parseObject(graphText);	
		JSONArray results=root.getJSONArray("results");
		for(int i=0;i<results.size();i++)
		{
			JSONObject result=results.getJSONObject(i);
			JSONArray data=result.getJSONArray("data");
			for(int j=0;j<data.size();j++)
			{
				JSONObject graph=data.getJSONObject(j).getJSONObject("graph");
				JSONArray relationships=graph.getJSONArray("relationships");
				TreeSet<String> edgeSet=new TreeSet<String>();
				for(int k=0;k<relationships.size();k++)
				{
					JSONObject relationship=relationships.getJSONObject(k);
					String startNode=relationship.getString("startNode");
					String endNode=relationship.getString("endNode");
					if(edgeSet.contains(startNode))
					{
						edgeSet.remove(startNode);
					}
					else
					{
						edgeSet.add(startNode);
					}
					if(edgeSet.contains(endNode))
					{
						edgeSet.remove(endNode);
					}
					else
					{
						edgeSet.add(endNode);
					}
				}
				String id1=edgeSet.first();
				String id2=edgeSet.last();
				mergeNodes(id1,id2);
			}
		}
	}

	/**
	 * 某个公司节点在页面上被删除后，删除此公司对应的其他信息，包括
	 * 1、old2New
	 * 2、personName2CompanyIdSet
	 * 3、
	 * 4、
	 * 5、
	 */
	public void deleteCompany()
	{
		HashSet<String> deletePersonIdSet=new HashSet<String>();
		Iterator<Entry<String, String>> iterator = companyIdPersonName2PersonId.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<String, String> entry = iterator.next();
			String key=entry.getKey();
			String[] tempArr=key.split("\\|");
			String companyId=tempArr[0];
			String personName=tempArr[1];
			String personId=entry.getValue();	
			if(deleteCompanyIdSet.contains(companyId))
			{
				deletePersonIdSet.add(personId);
				iterator.remove();
				personName2CompanyIdSet.get(personName).remove(companyId);
				old2New.remove(personId);
				personIdArr1.remove(personId);
			}
		}

		HashMap<String,TreeSet<String>> tempMap=new HashMap<String,TreeSet<String>>();
		Iterator<Entry<String, String>> iterator2 = old2New.entrySet().iterator();
		while(iterator2.hasNext())
		{
			Entry<String, String> entry = iterator2.next();
			String oldId=entry.getKey();
			String newId=entry.getValue();
			if(deletePersonIdSet.contains(newId) && !deletePersonIdSet.contains(oldId))
			{
				if(!tempMap.containsKey(newId))
				{
					tempMap.put(newId, new TreeSet<String>());
				}
				tempMap.get(newId).add(oldId);
			}
		}
		
		Iterator<Entry<String, TreeSet<String>>> iterator3 = tempMap.entrySet().iterator();
		while(iterator3.hasNext())
		{
			Entry<String, TreeSet<String>> entry = iterator3.next();
			String id1=entry.getValue().first();
			if(entry.getValue().size()==1)
			{
				old2New.remove(id1);
			}
			else
			{
				for(String id2:entry.getValue())
				{
					old2New.put(id2, id1);
				}
			}
			
		}
		
		Iterator<Entry<String, String>> iterator4 = old2New.entrySet().iterator();
		while(iterator4.hasNext())
		{
			Entry<String, String> entry = iterator4.next();
			String oldId=entry.getKey();
			String newId=entry.getValue();
			mergeNodes(oldId, newId);
		}
	}
	
	/**
	 * 棱镜一下主方法
	 * @throws Exception 
	 */
	public void execFunc() throws Exception
	{
		deleteCompany();
		if(waitForMergeCompanyId.size()>0)
		{
			String query=String.format("match (c:Company)<-[r]-(p:Person) where id(c) in [%s] return r", 
					StringUtils.join(waitForMergeCompanyId, ",")
					);
//			System.out.println(query);
			String graphText= Neo4j.execute(query,"graph");	
//			System.out.println(graphText);
			JSONObject root = JSONObject.parseObject(graphText);	
			JSONArray results=root.getJSONArray("results");
			for(int i=0;i<results.size();i++)
			{
				
				JSONObject result=results.getJSONObject(i);
				JSONArray data=result.getJSONArray("data");
				
				for(int j=0;j<data.size();j++)
				{
					JSONObject graph=data.getJSONObject(j).getJSONObject("graph");
//					System.out.println("graph: "+graph);
					JSONArray nodes=graph.getJSONArray("nodes");
					for(int l=0;l<nodes.size();l++)
					{
						JSONObject node=nodes.getJSONObject(l);
						String id=node.getString("id");
						String label=node.getJSONArray("labels").getString(0);
						if(label.equals("Person"))
						{
							personId2Name.put(id, node.getJSONObject("properties").getString("姓名"));
						}
						else
						{
							companyId2Name.put(id, node.getJSONObject("properties").getString("公司名称"));
						}
					}
					
					JSONArray relationships=graph.getJSONArray("relationships");
					relationshipArr.add(graph);
					
					for(int k=0;k<relationships.size();k++)
					{
						JSONObject relationship=relationships.getJSONObject(k);
//						System.out.println("relationship: "+relationship);
						String personid=relationship.getString("startNode");
						String personName=personId2Name.get(personid);
						String company=relationship.getString("endNode");
						if(!personName2CompanyIdSet.containsKey(personName))
						{
							personName2CompanyIdSet.put(personName, new TreeSet<String>());
						}
						personName2CompanyIdSet.get(personName).add(company);
						companyIdPersonName2PersonId.put(company+"|"+personName, personid);
						personId2CompanyName.put(personid, companyId2Name.get(company));
					}
				}
			}
		}
		

		Iterator<Entry<String, TreeSet<String>>> iterator = personName2CompanyIdSet.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<String, TreeSet<String>> entry = iterator.next();
			String personName=entry.getKey();
			TreeSet<String> companyIdSet=entry.getValue();
			
			if(companyIdSet.size()>1)
			{
//				System.out.println(personName+" companyIdSet.size():"+companyIdSet.size());
				for(String companyId:companyIdSet)
				{
					String personId=companyIdPersonName2PersonId.get(companyId+"|"+personName);
					if(!personIdArr1.contains(personId))
					{
						personIdArr2.add(personId);
					}
					personIdArr1.add(personId);
				}
			}
			for(String company1:companyIdSet)
			{
				for(String company2:companyIdSet.tailSet(company1,false))
				{
					String couple=company1+"|"+company2;
					if(!couple2PersonNameSet.containsKey(couple))
					{
						couple2PersonNameSet.put(couple, new TreeSet<String>());
					}
					couple2PersonNameSet.get(couple).add(personName);
				}
			}
		}
		
		mergePersonNames();
		
		Iterator<Entry<String, TreeSet<String>>> iterator3 = couple2PersonNameSet.entrySet().iterator();
		while(iterator3.hasNext())
		{
			Entry<String, TreeSet<String>> entry = iterator3.next();
			
			TreeSet<String> personNameSet=entry.getValue();
			if(personNameSet.size()>1)
			{
				String[] couple=entry.getKey().split("\\|");
				String companyId1=couple[0];
				String companyId2=couple[1];
				for(String personName:personNameSet)
				{
					String id1=companyIdPersonName2PersonId.get(companyId1+"|"+personName);
					String id2=companyIdPersonName2PersonId.get(companyId2+"|"+personName);
					mergeNodes(id1,id2);
				}
			}
		}
		
		Iterator<Entry<String, Integer>> iterator2 = groupIdMap.entrySet().iterator();
		while(iterator2.hasNext())
		{
			Entry<String, Integer> entry = iterator2.next();
			String oldId=entry.getKey();
			int groupId=entry.getValue();
			String newId=groupList.get(groupId).first();
			old2New.put(oldId, newId);
		}
//		System.out.println(groupList);
//		for(TreeSet<String> group:groupList)
//		{
//			if(group!=null)
//			{
//				System.out.println("++++++++++++++++++++++++++++++++++++");
//				for(String personId:group)
//				{
//					String companyName=personId2CompanyName.getString(personId);
//					String personName=personId2Name.get(personId);
//					if(companyName!=null)
//					System.out.println(companyName+" -> "+personName);
//				}
//			}
//		}
//		for(int i=0;i<relationshipArr.size();i++)
//		{
//			JSONObject relationship=relationshipArr.getJSONObject(i);
//			String personId=relationship.getString("startNode");
//			if(!old2New.containsKey(personId))
//			{
//				relationshipArr.remove(i);
//				i--;
//			}
//		}
	}
	
	public static void main(String[] args) throws Exception
	{
		LengJingFunc func1=new LengJingFunc("[\"6247251\",\"55783128\"]");
//		System.out.println("getOld2NewText: "+func1.getOld2NewText());
//		func1.initPersonIdSet1();
//		func1.initOld2New();
		func1.execFunc();
//		func.initPersonName2CompanyIdSet(companyIdPersonName2PersonIdText);
//		System.out.println("func1.getRelationshipText(): "+func1.getRelationshipText());
//		System.out.println("getPersonIdSet1Text: "+func.getPersonIdSet1Text());
//		func1.getRelationshipText();
//		System.out.println(func1.getRelationshipText());
		System.out.println(func1.getOld2NewText());
//		System.out.println(func1.relationshipArr);
		LengJingFunc func2=new LengJingFunc("[\"17778065\",\"19675755\",\"24772789\",\"35740465\",\"44056218\",\"69924452\",\"6247251\",\"55783128\"]");
		/**
		 * 非首次调用需要执行以下3个方法
		 */
		func2.initCompanyIdPersonName2PersonId(func1.getCompanyIdPersonName2PersonIdText());
		func2.initOld2New(func1.getOld2NewText());
		func2.initPersonIdArr1(func1.getPersonIdArr1Text());
		func2.execFunc();
		System.out.println(func2.getOld2NewText());
		
		LengJingFunc func3=new LengJingFunc("[\"6247251\",\"55783128\"]");
		System.out.println("-------------------------------------");
		func3.initCompanyIdPersonName2PersonId(func2.getCompanyIdPersonName2PersonIdText());
		func3.initOld2New(func2.getOld2NewText());
		func3.initPersonIdArr1(func2.getPersonIdArr1Text());
		func3.execFunc();
		System.out.println(func3.getOld2NewText());
	}
}
