package com.net.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class LengJingFunction {

	/**
	 * 1、根据图上所有公司节点id，找出所有有重名的自然人节点
	 * 2、通过黑名单进行过滤，对过滤后的人名进行合并计算
	 * 3、根据多人同时任职的关系对不能合并的节点进行合并
	 * 
	 * @param companyNodes 当前图上全部节点id,逗号分隔
	 * @param old2NewStr 已知的节点合并关系，oldId:newId,oldId:newId,...
	 * @param blackListStr 已计算过的姓名列表，逗号分隔
	 * @return :
	 * @throws Exception 
	 * 
	 */
	
	public static JSONObject lengJing(String companyNodes,String old2NewStr,String blackListStr) throws Exception
	{
		
		JSONObject res=new JSONObject();
		JSONObject old2NewJSON=new JSONObject();
		JSONArray blackListArr=new JSONArray();
		//已经计算过的姓名集合，在此名单中的姓名不需要重新计算
		HashSet<String> blackList=new HashSet<String>();
		//key：合并前自然人id，val：合并后自然人id
		HashMap<String,String> old2New=new HashMap<String,String>();

		if(blackListStr.length()>0)
		{
			for(String person:blackListStr.split(","))
			{
				blackList.add(person);
			}
		}
		
		if(old2NewStr.length()>0)
		{
			for(String idCoupleStr:old2NewStr.split(","))
			{
//				System.out.println("<"+idCoupleStr+">");
				String[] idCouple=idCoupleStr.split(":");
				old2New.put(idCouple[0],idCouple[1]);
			}
		}
		
		
		// key：自然人姓名，val：关联公司集合
		Map<String,TreeSet<String>> personName2CompanyIdSet=new TreeMap<String,TreeSet<String>>();
		//key：自然人id，val：自然人姓名
		HashMap<String,String> personId2Name=new HashMap<String,String>();
		//key：公司id，val：公司名称
		HashMap<String,String> companyId2Name=new HashMap<String,String>();
		//key：公司组合，val：同时关联的自然人列表
		HashMap<String,TreeSet<String>> couple2PersonIdSet=new HashMap<String,TreeSet<String>>();
		//key：公司id+'|'+自然人姓名，val：自然人id
		HashMap<String,String> companyIdPersonName2PersonId=new HashMap<String,String>();
		//key：自然人id，val：groupId
		HashMap<String,Integer> groupIdMap=new HashMap<String,Integer>();
		ArrayList<TreeSet<String>> groupList=new ArrayList<TreeSet<String>>();
		
		
		
		StringBuilder duplicatePersonNames=new StringBuilder();
		String query=String.format("match (c:Company)<-[r]-(p:Person) where id(c) in [%s] return r", companyNodes);
//		System.out.println(query);
		String graphText= Neo4j.execute(query,"graph");	
		JSONObject root = JSONObject.parseObject(graphText);	
//		System.out.println(root);
		JSONArray results=root.getJSONArray("results");
		for(int i=0;i<results.size();i++)
		{
			JSONObject result=results.getJSONObject(i);
			JSONArray data=result.getJSONArray("data");
			
			for(int j=0;j<data.size();j++)
			{
//				System.out.println("---------------------------------------------------------");
				JSONObject graph=data.getJSONObject(j).getJSONObject("graph");
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
//				System.out.println(nodes);
				for(int k=0;k<relationships.size();k++)
				{
					JSONObject relationship=relationships.getJSONObject(k);
					String personid=relationship.getString("startNode");
					String personName=personId2Name.get(personid);
					String company=relationship.getString("endNode");
					if(!personName2CompanyIdSet.containsKey(personName))
					{
						personName2CompanyIdSet.put(personName, new TreeSet<String>());
					}
					personName2CompanyIdSet.get(personName).add(company);
					companyIdPersonName2PersonId.put(company+"|"+personName, personid);
				}
			}
		}
		
		Iterator<Entry<String, TreeSet<String>>> iterator = personName2CompanyIdSet.entrySet().iterator();
		while(iterator.hasNext())
		{
			Entry<String, TreeSet<String>> entry = iterator.next();
			String personName=entry.getKey();
			TreeSet<String> companySet=entry.getValue();
			if(companySet.size()>1 && !blackList.contains(personName))
			{
				duplicatePersonNames.append(personName+",");
				blackList.add(personName);
			}
			for(String company1:companySet)
			{
				for(String company2:companySet.tailSet(company1,false))
				{
					String couple=company1+"|"+company2;
					if(!couple2PersonIdSet.containsKey(couple))
					{
						couple2PersonIdSet.put(couple, new TreeSet<String>());
					}
					couple2PersonIdSet.get(couple).add(personName);
//					System.out.println(personName+" -> "+couple);
//					coupleMap.put(couple, coupleMap.containsKey(couple)?coupleMap.get(couple)+1:1);
				}
			}
		}
		
		if(duplicatePersonNames.length()>0)
		{
			duplicatePersonNames.setLength(duplicatePersonNames.length()-1);
		}
		/**/
//		mergePersons(duplicatePersonNames.toString(),old2New);
		String personNames="'"+duplicatePersonNames.toString().replace(",", "','")+"'";
//		HashMap<String,String> old2New=new HashMap<String,String>();
		String query2=String.format("with [%s] as arr "
				+ "match p=allShortestPaths((n1:Person)-[*1..3]-(n2:Person))  "
				+ "where n1.姓名 in arr and n2.姓名 in arr and n1.姓名=n2.姓名 and id(n1)>id(n2) "
				+ "return p "
				, personNames);
//		System.out.println(query);
		String graphText2= Neo4j.execute(query2,"graph");
		JSONObject root2 = JSONObject.parseObject(graphText2);	
		JSONArray results2=root2.getJSONArray("results");
		for(int i=0;i<results2.size();i++)
		{
			JSONObject result=results.getJSONObject(i);
			JSONArray data=result.getJSONArray("data");
			for(int j=0;j<data.size();j++)
			{
//				System.out.println("---------------------------------------------------------");
				JSONObject graph=data.getJSONObject(j).getJSONObject("graph");
				JSONArray relationships=graph.getJSONArray("relationships");
//				System.out.println(graph);
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
		}

		/**/
		
		Iterator<Entry<String, TreeSet<String>>> iterator3 = couple2PersonIdSet.entrySet().iterator();
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
		
		blackListArr.addAll(blackList);
		old2NewJSON.putAll(old2New);		
//		System.out.println(blackListArr);
//		System.out.println(old2NewJSON);
		res.put("old2New", old2NewJSON);
		res.put("blackList", blackListArr);
		return res;
	}
	
	public static void main(String[] args) throws Exception
	{
		System.out.println(lengJing("2518026,4225969,30969564,14255946","",""));
	}
}
