package com.atguigu.weibo;

import jdk.nashorn.internal.codegen.Namespace;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;


/**
 * @author xuzl
 * @create 2019-06-30 14:40
 */
public class WeiBoUtil {

    private static Configuration configuration = HBaseConfiguration.create();
    
    static {
        configuration.set("hbase.zookeeper.quorum","192.168.25.102");
    }

    //创建命名空间
    public static void createNamespace(String ns) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //创建NS描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        //创建操作
        admin.createNamespace(namespaceDescriptor);
        //关闭资源
        admin.close();
        connection.close();
    }

    //创建表
    public static void createTable(String tableName,int versions,String ...cfs ) throws IOException {
        //创建连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        //构建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //循环添加列族
        for(String cf : cfs){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        //关闭资源
        admin.close();
        connection.close();

    }

    /**
     * 1、更新微博内容表数据
     * 2、更新收件箱表数据
     *     --获取当前操作人的fans
     *     --去往收件箱表依次更新数据
     * @param uid
     * @param content
     * @throws IOException
     */
    //发布微博
    public static void createData(String uid,String content) throws IOException {
        //获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取三张操作表对象
        Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
        Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
        Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
        //拼接RK
        long ts=System.currentTimeMillis();
        String rowKey =uid+"_"+ts;
        //生成put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("content"),Bytes.toBytes(content));
        //往内容表添加数据
        conTable.put(put);
        //获取关系表中的fans
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes("fans"));
        Result result = relaTable.get(get);
        Cell[] cells = result.rawCells();
        if(cells.length <=0){
            return;
        }
        //更新fans收件箱表
        ArrayList<Put> puts = new ArrayList<Put>();
        for(Cell cell : cells){
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            Put inboxPut = new Put(cloneQualifier);
            inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),Bytes.toBytes(rowKey));
            puts.add(inboxPut);
        }
        inboxTable.put(put);
        //关闭资源
        inboxTable.close();
        relaTable.close();
        conTable.close();
        connection.close();


    }

    //关注用户

    //取关用户

    //获取微博内容(初始化页面)

    //获取微博内容(查看某个人所有微博内容)
}
