package com.atguigu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author xuzl
 * @create 2019-06-29 16:31
 */
public class TestHbase {
    private static Admin admin = null;
    private static Connection connection=null;
    private static Configuration configuration = null;
    static{
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "192.168.25.102");

        try {
            //获取连接对象
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void close(Connection conn,Admin admin){
        if(conn!=null){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin !=null){
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //判断表是否存在
    public static boolean tableExist(String tableName) throws IOException {
        //Hbase配置文件
        //HBaseConfiguration configuration = new HBaseConfiguration();

        //获取Hbase管理员对象
        //HBaseAdmin admin = new HBaseAdmin(configuration);
        //执行
        //boolean tableExists = admin.tableExists(tableName);
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
        //关闭资源
        admin.close();
        return tableExists;
    }

    //创建表
    public static  void createTable(String tableName,String ... cfs) throws IOException {
        if(tableExist(tableName)){
            System.out.println("表已存在");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        for(String cf : cfs){
            //创建列描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            //hColumnDescriptor.setMaxVersions(3);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //创建表操作
        admin.createTable(hTableDescriptor);
        System.out.println("表创建成功");
    }


    //删除表
    public static void deleteTable(String tableName) throws IOException {
        if(!tableExist(tableName)){
            return;
        }
        //使表不可用(下线)
        admin.disableTable(TableName.valueOf(tableName));
        //执行删除操作
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("表已删除");
    }

    //增、改
    public static void putData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //获取表对象
        //HTable table = new HTable(configuration,TableName.valueOf(tableName));
        //创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //添加数据
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        //执行添加操作
        table.put(put);
        table.close();
    }

    //删
    public static void delete (String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));//删除所有版本,建议使用
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));//删除最新版本
        //执行删除资源
        table.delete(delete);
        table.close();

    }

    //查
    //全表扫描
    public static void scanTable(String tableName) throws IOException {
        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //构建扫描器
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        //遍历数据打印
        for(Result result : results){
            Cell[] cells = result.rawCells();
            //byte[] row = result.getRow();
            for(Cell cell :cells){
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                        +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",VALUE:"+Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }
        table.close();
    }
    //获取指定列族：列的数据
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //获取table对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //创建一个Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        get.setMaxVersions();

        Result result = table.get(get);
            Cell[] cells = result.rawCells();
            //byte[] row = result.getRow();
            for(Cell cell :cells){
                System.out.println("RK:"+Bytes.toString(CellUtil.cloneRow(cell))
                        +",CF:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",VALUE:"+Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
            table.close();
    }

    public static void main(String[] args) throws IOException {
        System.out.println(tableExist("student"));
        System.out.println(tableExist("staff"));
        createTable("staff","info");
        System.out.println(tableExist("staff"));
        deleteTable("staff");
        putData("student","1003","info","name","zhangsan");
        delete("student","1002","info","name");
        scanTable("student");
        getData("student","1003","info","name");
        close(connection,admin);

    }
}
