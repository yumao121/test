package com.sxt.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {

	Configuration conf;
	HBaseAdmin admin;
	HTable htable;
	byte[] family = "cf".getBytes();
	
	// 1、删除 cell？？
	// 2、表设计
	
	
	String TN = "phone";  //表名称
	
	@Before
	public void begin() throws Exception {
		conf = new Configuration();
		
		// 伪分布式hbase  zk列表指定当前hbase那台服务器   完全分布式写node02,node03,node04
		conf.set("hbase.zookeeper.quorum", "node01");
		
		admin = new HBaseAdmin(conf);
		htable = new HTable(conf, TN);
	}
	
	@After
	public void end() throws Exception {
		if(admin != null) {
			admin.close();
		}
		if(htable != null) {
			htable.close();
		}
	}

	@Test
	public void createTbl() throws Exception {
		if(admin.tableExists(TN)) {
			admin.disableTable(TN);
			admin.deleteTable(TN);
		}
		
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));

		HColumnDescriptor cf = new HColumnDescriptor("cf");
		cf.setInMemory(true);
		cf.setMaxVersions(1);
		
		desc.addFamily(cf);
		
		admin.createTable(desc);
	}
	
	@Test
	public void insertDB1() throws Exception {
		String rowkey = "123";
		
		Put put = new Put(rowkey.getBytes());
		put.add("cf".getBytes(), "name".getBytes(), "xiaoming".getBytes());
		put.add("cf".getBytes(), "sex".getBytes(), "man".getBytes());
		
		htable.put(put);
	}
	
	@Test
	public void getDB1() throws Exception {
		String rowkey = "123";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf".getBytes(), "name".getBytes());
		
		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());
		
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}
	
	/**
	 * 通话详单 
	 * 包含：手机号，对方手机号，日期，通话时长，主叫被叫类型...
	 * 
	 * Rowkey设计：手机号_(Long.Max-时间戳)
	 * 
	 * 1、查询某个月份 的  所有的通话详单  时间降序
	 * 
	 * 2、查询某个手机号   所有主叫类型   通话记录
	 * @throws Exception
	 */
	
	HTools t = new HTools();
	
	Random r = new Random();

	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	
	/**
	 * 生成测试数据
	 * 
	 * 十个用户 产生一百条通话记录 
	 */
	@Test
	public void insertDB2() throws Exception {
		
		List<Put> puts = new ArrayList<Put>();
		
		for (int i = 0; i < 10; i++) {
			String pnum = t.getPhoneNum("186");
			
			for (int j = 0; j < 100; j++) {
				String dnum = t.getPhoneNum("177");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				String rowkey = pnum + "_" + (Long.MAX_VALUE-sdf.parse(datestr).getTime());
				
				Put put = new Put(rowkey.getBytes());

				put.add(family, "date".getBytes(), datestr.getBytes());
				put.add(family, "length".getBytes(), length.getBytes());
				put.add(family, "type".getBytes(), type.getBytes());
				
				puts.add(put);
			}
		}
		
	}

	/**
	 * 生成测试数据
	 * 
	 * 十个用户   每天产生一百条通话记录 
	 */
	@Test
	public void insertDB3() throws Exception {
		
		List<Put> puts = new ArrayList<Put>();
		
		for (int i = 0; i < 10; i++) {
			String pnum = t.getPhoneNum("186");
			
			String day = "20180115";
			
			Call.dayCallDetail.Builder dayCall = Call.dayCallDetail.newBuilder();
			
			// 每个用户  这一天 产生的一百条通话记录
			for (int j = 0; j < 100; j++) {
				
				String dnum = t.getPhoneNum("177");
				String datestr = t.getDate2(day);
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				
				Call.callDetail.Builder callDetail = Call.callDetail.newBuilder();
				callDetail.setDnum(dnum);
				callDetail.setDate(datestr);
				callDetail.setLength(length);
				callDetail.setType(type);
				
				dayCall.addCallDetails(callDetail);
			}
			
			String rowkey = pnum + "_" + (Long.MAX_VALUE-sdf.parse("20180115000000").getTime());
			
			Put put = new Put(rowkey.getBytes());
			put.add(family, "call".getBytes(), dayCall.build().toByteArray());
			
			puts.add(put);
		}
		
		htable.put(puts);
	}
	
	/**
	 * 查询某个手机号 一天的所有通话记录
	 * 18697862438_9223370520909175807
	 */
	@Test
	public void getDB2() throws Exception {
		String rowkey = "18697862438_9223370520909175807";
		Get get = new Get(rowkey.getBytes());
		get.addColumn("cf".getBytes(), "call".getBytes());
		
		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell("cf".getBytes(), "call".getBytes());
		
		Call.dayCallDetail dayCall = Call.dayCallDetail.parseFrom(CellUtil.cloneValue(cell));

		for(Call.callDetail call : dayCall.getCallDetailsList()) {
			System.out.println(call.getDate() + " - " + call.getDnum() + " - " + call.getType() + " - " + call.getLength());
		}
	}
	
	/**
	 * 查询某个手机号  某个月份所有的通话记录
	 * 范围
	 * @throws Exception
	 */
	@Test
	public void scanDB1() throws Exception {
		Scan scan = new Scan();
		
		String pnum = "18692739289_";

		String startRowkey = pnum + (Long.MAX_VALUE-sdf.parse("20181001000000").getTime());
		String stopRowkey = pnum + (Long.MAX_VALUE-sdf.parse("20180901000000").getTime());
		
		scan.setStartRow(startRowkey.getBytes());
		scan.setStopRow(stopRowkey.getBytes());
		
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
	}
	
	/**
	 * 查询某个手机号  所有的主叫type=1
	 * 过滤器
	 * @throws Exception 
	 */
	@Test
	public void scanDB2() throws Exception {
		Scan scan = new Scan();
		
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		
		PrefixFilter filter1 = new PrefixFilter("18692739289".getBytes());
		list.addFilter(filter1);
		
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(family, 
				"type".getBytes(), CompareOp.EQUAL, "1".getBytes());
		
		list.addFilter(filter2);
		
		scan.setFilter(list);
		
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "dnum".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "date".getBytes()))));
			System.out.print(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "type".getBytes()))));
			System.out.println(" - " + new String(CellUtil.cloneValue(rs.getColumnLatestCell(family, "length".getBytes()))));
		}
		
	}
	
}