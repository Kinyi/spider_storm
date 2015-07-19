package cn.usth.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class HbaseUtil {
	/**
	 * HBASE 表名称
	 */
	public static final String TABLE_NAME = "spider";
	/**
	 * 列簇1 商品信息
	 */
	public static final String COLUMNFAMILY_1 = "goodsinfo";
	/**
	 * 列簇1中的列
	 */
	public static final String COLUMNFAMILY_1_DATA_URL = "data_url";
	public static final String COLUMNFAMILY_1_PIC_URL = "pic_url";
	public static final String COLUMNFAMILY_1_TITLE = "title";
	public static final String COLUMNFAMILY_1_PRICE = "price";
	/**
	 * 列簇2 商品规格
	 */
	public static final String COLUMNFAMILY_2 = "spec";
	public static final String COLUMNFAMILY_2_PARAM = "param";
	
	static{
		File workaround = new File(".");
		System.getProperties().put("hadoop.home.dir",
				workaround.getAbsolutePath());
		new File("./bin").mkdirs();
		try {
			new File("./bin/winutils.exe").createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	

	public static void main(String[] args) throws IOException {
		

		// // HbaseUtils.create(tableName, columnFamily);
		// HbaseUtils.put(tableName, "1217494", columnFamily, "url",
		// "http://1111/");
		// HbaseUtils.put(tableName, "1217494", columnFamily, "title",
		// "iphone 5s");
		// HbaseUtils.put(tableName, "1217494", columnFamily, "url",
		// "http://1111/picpath");
		// HbaseUtils.put(tableName, "row2", columnFamily, "cl1", "123");
		// HbaseUtils.put(tableName, "row3", columnFamily, "cl1", "789");
		// HbaseUtils.put(tableName, "row4", columnFamily, "cl1", "abc");

		// ScanWithFilter(tableName, columnFamily1, "data_url");
		//Goods goods = get(TABLE_NAME, "jd_972627");
		//System.err.println(goods.toString());
		 HbaseUtil.scan(TABLE_NAME);
		// HbaseUtils.delete(tableName);
	}

	// hbase操作必备
	private static Configuration getConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://192.168.80.100:8020/hbase");
		// 使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "192.168.80.100");
		return conf;
	}

	// 创建一张表
	public static void create(String tableName, String... columnFamily)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			System.out.println("table exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
//			tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			//add by myself
			for (String cf : columnFamily) {
				tableDesc.addFamily(new HColumnDescriptor(cf));
			}
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		}
		admin.close();
	}

	// 添加一条记录
	public static void put(String tableName, String row, String columnFamily,
			String column, String data) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "'," + columnFamily + ":" + column
				+ "','" + data + "'");
		table.close();
	}

	// 读取一条记录
	@SuppressWarnings({ "resource" })
	public static Goods get(String tableName, String row) {
		HTable table;
		Goods goods = null;
		try {
			table = new HTable(getConfiguration(), tableName);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			KeyValue[] raw = result.raw();
			if (raw.length == 5) {
				goods = new Goods();
				goods.setId(row);
				goods.setDataurl(new String(raw[0].getValue()));
				goods.setPicpath(new String(raw[1].getValue()));
				String str = new String(raw[2].getValue());
				if(str==null||"".equals(str)){
					str="0.00";
				}
				goods.setPrice(Float.parseFloat(str));
				goods.setName(new String(raw[3].getValue()));
				goods.setSpec(new String(raw[4].getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return goods;
	}

	// 显示所有数据
	public static void scan(String tableName) throws IOException {
		HTable table = new HTable(getConfiguration(), tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Scan: " + result);
		}
		table.close();
	}

	// 删除表
	public static void delete(String tableName) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(getConfiguration());
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Delete " + tableName + " 失败");
			}
		}
		System.out.println("Delete " + tableName + " 成功");
		admin.close();
	}

	// 过滤器
	public static void ScanWithFilter(String tableName, String cf, String column) {
		HTable table = null;
		try {
			table = new HTable(getConfiguration(), tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		Scan scan = new Scan();
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes(cf), Bytes.toBytes(column), CompareOp.EQUAL,
				Bytes.toBytes("http://item.jd.com/1220040.html"));
		list.addFilter(filter1);

		scan.setFilter(list);
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (Result result : scanner) {
			//对应hadoop2版本的hbase
			/*byte[] valueArray = result.getColumnLatestCell(Bytes.toBytes(cf),
					Bytes.toBytes(column)).getValueArray();*/
			byte[] valueArray = result.getValue(Bytes.toBytes(cf), Bytes.toBytes(column));
			System.out.println("Scan: " + Bytes.toString(valueArray));
		}
	}
	/**
	 * 查看某一列的多个版本的值
	 * @param goodid 商品ID
	 * @param family 列簇
	 * @param qualifier 列名
	 * @param id 
	 * @return
	 */
	@SuppressWarnings({ "resource" })
	public static List<Map<String,String>> getCellMoreVersion(String tableName,String family,String qualifier, String id){
		List<Map<String,String>> list = new ArrayList<Map<String,String>>();
		try {
			HTable table = new HTable(getConfiguration(), tableName);
			Get get = new Get(Bytes.toBytes(id));
			get.setMaxVersions();
			Result result = table.get(get);
			//对应hadoop2版本的hbase
			/*java.util.List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			for (int i = 0; i < columnCells.size(); i++) {
				Cell cell = columnCells.get(i);
				long timestamp = cell.getTimestamp();
				Map<String,String> map = new HashMap<String, String>();
				map.put("time", MyDateUtils.formatDate4(new Date(timestamp)));
				map.put("value", new String(cell.getValue()));
				list.add(map);
			}*/
			List<KeyValue> columnCells = result.getColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			for (int i = 0; i < columnCells.size(); i++) {
				KeyValue keyValue = columnCells.get(i);
				long timestamp = keyValue.getTimestamp();
				Map<String,String> map = new HashMap<String, String>();
				map.put("time", MyDateUtils.formatDate4(new Date(timestamp)));
				map.put("value", new String(keyValue.getValue()));
				list.add(map);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return list;
	}
}
