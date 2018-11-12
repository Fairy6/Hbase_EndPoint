package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {

	public static void main(String[] args) throws IOException {
		CourseFilter("001");
		StudentFilter("1");
		TeacherFilter("t1");
		MaxStudent();
		LessStudent();

	}

	public static final String Student = "student1";
	public static final String Student_Column = "info";
	public static final String Student_Col_Course = "course";
	public static final String Student_Name = "name";
	public static final String Student_Age = "age";
	public static final String Student_Sex = "sex";
	
	public static final String Course = "course";
	public static final String Course_Column = "info";
	public static final String Course_Col_Stu = "student";
	public static final String Course_Title = "title";
	public static final String Course_Introduction = "introduction";
	public static final String Course_Teacher_id = "teacher_id";
	
	
	
 	 //=========================connecation and congriguration img============
	  public static HTablePool pool;
  	/*
  	 * 连接池配置
  	 */
  	public static Configuration configuration;
  	/*
  	 * 连接池对象
  	 */
      public static Connection connection;
      private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
  	private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
      /*
  	 * Admin
  	 */
      public static Admin admin;
      /**
  	 * HBase位置
  	 */
  	@SuppressWarnings("unused")
  	private static final String HBASE_POS = "localhost";

  	/**
  	 * ZooKeeper位置
  	 */
  	private static final String ZK_POS = "localhost";

  	/**
  	 * zookeeper服务端口
  	 */
  	private final static String ZK_PORT_VALUE = "2181";
      /*
       * 建立连接
       */
     //==================================================================
     //=====================contry open and close========================
      public static void init(){
          configuration  = HBaseConfiguration.create();
          configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
          configuration.set(ZK_QUORUM, ZK_POS);
  		configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
          try{
              connection = ConnectionFactory.createConnection(configuration);
              //Test
              pool = new HTablePool(configuration, 1000); 
              admin = connection.getAdmin();
          }catch (IOException e){
              e.printStackTrace();
          }
      }
      /*
       * 关闭连接
       */
      public static void close(){
          try{
              if(admin != null){
                  admin.close();
              }
              if(null != connection){
                  connection.close();
              }
          }catch (IOException e){
              e.printStackTrace();
          }
      }
 	public static void CourseFilter(String student_id) throws IOException{
		init();
 		Table table1 = connection.getTable(TableName.valueOf(Course));
		Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(student_id));
		
		Scan scan1 = new Scan();
		scan1.setFilter(filter1);
		
		ResultScanner ss1 = table1.getScanner(scan1);
		List<String> rowkeyList = new ArrayList<String>();
		List<String> courseList = new ArrayList<String>();
		
		if(ss1 != null){
			for(Result r : ss1){
				for(KeyValue kv : r.raw()){
					System.out.println(kv);
					//获取ｋｅｙ
					rowkeyList.add(new String(kv.getRow()));
				}
			}
		}
		System.out.println("===========根据学号student_id查询学生选课编号course_id和名称title==========");
		System.out.println("学号为：" + student_id + "的学生查询的course_id的结果为");
		System.out.println(rowkeyList); 
		ss1.close();

		for(String row : rowkeyList){
			Get get = new Get(Bytes.toBytes(row));
			get.addColumn(Bytes.toBytes(Course_Column), Bytes.toBytes(Course_Title));
			//get.addColumn(family, qualifier)
			Result result = table1.get(get);
			for(KeyValue kv:result.raw()){
				courseList.add(new String(kv.getValue()));
			}
		}
		System.out.println("选修的课程的title为：");
		System.out.println(courseList);	
	}
 	
 	
 	
 	
 	public static void StudentFilter(String course_id) throws IOException{
 		init();
 		Table table2 = connection.getTable(TableName.valueOf(Student));
 		Table table1 = connection.getTable(TableName.valueOf(Course));
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(course_id));
		List<String> courseList = new ArrayList<String>();
		List<String> stuList = new ArrayList<String>();
		List<String> stuListName = new ArrayList<String>();
		Scan scan = new Scan();
		scan.setFilter(filter1);
		 ResultScanner scanner1 = table1.getScanner(scan);
		 System.out.println("=========根据课程号course_id查询选课学生学号student_id和姓名name=============");
		 for (Result res : scanner1) {
             //System.out.println(res);
			 courseList.add(new String(res.getRow()));
     }
     scanner1.close();
     
     for(String row : courseList){
			Get get = new Get(Bytes.toBytes(row));
			//get.addColumn(Bytes.toBytes(Course_Col_Stu), Bytes.toBytes("student"));
			//get.addColumn(family, qualifier)
			get.addFamily(Bytes.toBytes(Course_Col_Stu));
			Result result = table1.get(get);
			for(KeyValue kv:result.raw()){
				stuList.add(new String(kv.getValue()));
			}
		}
     System.out.println("选修课程号为:"+course_id + "的学生学号为：");
		System.out.println(stuList);	
     
     for(String s : stuList){
    	 Get get = new Get(Bytes.toBytes(s));
    	 get.addColumn(Bytes.toBytes(Student_Column ),  Bytes.toBytes("name"));
    	 Result result1=table2.get(get);
	        for(KeyValue kv1 : result1.raw()){  
	        	stuListName.add(new String(kv1.getValue()));
	         }  
     }
     System.out.println("学生名字为：");
		System.out.println(stuListName);	
 	}
 	
 	public static void TeacherFilter(String teacher_id) throws IOException{
 		
 		init();
 		Table table1 = connection.getTable(TableName.valueOf(Course));
		Filter filter1 = new ValueFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(teacher_id));
		
		Scan scan1 = new Scan();
		scan1.setFilter(filter1);
		
		ResultScanner ss1 = table1.getScanner(scan1);
		List<String> rowkeyList = new ArrayList<String>();
		List<String> courseList = new ArrayList<String>();
		
		if(ss1 != null){
			for(Result r : ss1){
				for(KeyValue kv : r.raw()){
					System.out.println(kv);
					//获取ｋｅｙ
					rowkeyList.add(new String(kv.getRow()));
				}
			}
		}
		System.out.println("===========根据老师编号查询学生选课编号course_id和名称title==========");
		System.out.println("老师编号为：" + teacher_id + "的授课的course_id的结果为");
		System.out.println(rowkeyList); 
		ss1.close();

		for(String row : rowkeyList){
			Get get = new Get(Bytes.toBytes(row));
			get.addColumn(Bytes.toBytes(Course_Column), Bytes.toBytes(Course_Title));
			//get.addColumn(family, qualifier)
			Result result = table1.get(get);
			for(KeyValue kv:result.raw()){
				courseList.add(new String(kv.getValue()));
			}
		}
		System.out.println("授课的title为：");
		System.out.println(courseList);	
	}
 	
 	
 	public static void MaxStudent() throws IOException {
 		init();
 		Table table = connection.getTable(TableName.valueOf(Student));
 		Table table2 = connection.getTable(TableName.valueOf(Course));
 		
 		List<String> studentidList = new ArrayList<String>();
 		int num = 0;
 		String studentid ="";
 		Scan scan = new Scan();
 		ResultScanner rs = table.getScanner(scan);
 		for(Result result : rs){
 			studentidList.add(new String(result.getRow()));
 			int tempnum = 0;
 			for(KeyValue keyvalue:result.raw()){
 				if(new String(keyvalue.getFamily()).equals("course")){
 					tempnum++;
 				}
 			}
 			if(tempnum>num){
 				num = tempnum;
 			}
 		}
 		System.out.println();
 		System.out.println("stuidlist:"+studentidList);
        System.out.println("morenum:"+num);
        
        System.out.println("scan student通过id查找..........................................................");

        Map<String,Integer> coursecount = new HashMap<String,Integer>();
 		for(String stuid :studentidList){
 			Get get = new Get(Bytes.toBytes(stuid));
 			get.addFamily(Bytes.toBytes("course"));
 			Result result = table.get(get);
 			int count = 0;
 			for(KeyValue keyvalue:result.raw()){
 				count++;
 			}
 			coursecount.put(stuid, count);
 		}
 		for(Map.Entry<String, Integer> entry:coursecount.entrySet()){
 			if(num == entry.getValue()){
 				studentid = entry.getKey();
 				System.out.println("上课最多的学生为："+studentid+",上了"+num+"门课。");
 			}
 		}
 	}
 	public static void LessStudent() throws IOException{
 		Table table = connection.getTable(TableName.valueOf(Student));
 		Table table2 = connection.getTable(TableName.valueOf(Course));
 		List<String> studentidList = new ArrayList<String>();
 		int num =0;
 		String studentid = "";
 		Scan scan = new Scan();
 		ResultScanner resultScanner = table.getScanner(scan);
 		for(Result result:resultScanner){
 			studentidList.add(new String(result.getRow()));
 			int tempnum = 0;
 			for(KeyValue kv:result.raw()){
 				if(new String(kv.getFamily()).equals("course")){
 					tempnum++;
 				}
 			}
 			num = tempnum;
 		}
 		ResultScanner rs = table.getScanner(scan);
 		for(Result result:rs){
 			int tempnum = 0;
 			for(KeyValue kv:result.raw()){
 				if(new String(kv.getFamily()).equals("course")){
 					tempnum++;
 				}
 			}
 			if(tempnum<num){
                num=tempnum;
           }

 		}
 		System.out.println("stuidlist:"+studentidList);
        System.out.println("lessnum:"+num);
        
        System.out.println("scan studernt通过id查找..........................................................");

        Map<String, Integer> coursecount= new HashMap<String,Integer>();
        for (String stuid : studentidList) {
             Get get=new Get(Bytes.toBytes(stuid));
             get.addFamily(Bytes.toBytes("course"));
             Result result=table.get(get);
             int count=0;
             for(KeyValue keyValue:result.raw()){
                  count++;
             }
             coursecount.put(stuid, count);
        }    
        for(Map.Entry<String, Integer> entry:coursecount.entrySet()){
//           lessnum=entry.getValue();
             if(num==entry.getValue()){
//                number=entry.getValue();
                  studentid=entry.getKey();
                  System.out.println("上课最少的学生为："+studentid+",上了"+num+"门课。");

        
 	}
        }
       
 	}
 	
}



