package com.atguigu.es;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author taylor
 * @create 2019-06-06 6:47
 */
public class App {
    private TransportClient client;

    //获取客户端对象
    @Test
    public void getClient() throws UnknownHostException {
        Settings settings =Settings.builder().put("cluster.name","my-application").build();
        //获取客户端对象
        client=new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop102"), 9300));

        // 3 打印集群名称
        System.out.println(client.toString());

    }

    //创建索引
    @Test
    public void createIndex(){
        // 1 创建索引
        client.admin().indices().prepareCreate("blog2").get();

        // 2 关闭连接
        client.close();

    }
    //删除索引
    @Test
    public void deleteIndex() {
        //删除索引
        client.admin().indices().prepareDelete("blog").get();
        // 关闭资源
        client.close();

    }

    //创建文档以json形式
    @Test
    public void createIndexbyJson(){
        //创建文档内容
        String json= "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";
        //创建
        IndexResponse response=client.prepareIndex("blog","article","1").setSource(json).execute().actionGet();
        //打印返回值
        System.out.println("索引:"+response.getIndex());
        System.out.println("类型:"+response.getType());
        System.out.println("id:"+response.getId());
        System.out.println("版本号"+response.getVersion());
        System.out.println("结果:"+response.getResult());
        client.close();
    }

    //创建文档以hashmap
    @Test
    public void createIndexByMap(){
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("id", "2");
        json.put("title", "基于Lucene的搜索服务器");
        json.put("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        IndexResponse indexResponse = client.prepareIndex("blog", "article", "2").setSource(json).execute().actionGet();
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();

    }
    //创建文档以builder
    @Test
    public void createIndexBuilder() throws Exception {

        // 1 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id", 3)
                .field("title", "基于Lucene的搜索服务器").field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。")
                .endObject();

        // 2 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog", "article", "3").setSource(builder).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }

    //单个索引查询
    @Test
    public void getData() throws Exception {

        // 1 查询文档
        GetResponse response = client.prepareGet("blog", "article", "1").get();

        // 2 打印搜索的结果
        System.out.println(response.getSourceAsString());

        // 3 关闭连接
        client.close();
    }

    //多个索引查询
    @Test
    public void getMultiData() {

        // 1 查询多个文档
        MultiGetResponse response = client.prepareMultiGet().add("blog", "article", "1").add("blog", "article", "2", "3")
                .add("blog", "article", "2").get();

        // 2 遍历返回的结果
        for(MultiGetItemResponse itemResponse:response){
            GetResponse getResponse = itemResponse.getResponse();

            // 如果获取到查询结果
            if (getResponse.isExists()) {
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }

        // 3 关闭资源
        client.close();
    }

   //更新文档
    @Test
    public void updateData() throws Throwable {

        // 1 创建更新数据的请求对象
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("3");

        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                // 对没有的字段添加, 对已有的字段替换
                .field("title", "基于Lucene的搜索服务器")
                .field("content",
                        "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                .field("createDate", "2017-8-22").endObject());

        // 2 获取更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        // 3 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("create:" + indexResponse.getResult());

        // 4 关闭连接
        client.close();
    }






}
