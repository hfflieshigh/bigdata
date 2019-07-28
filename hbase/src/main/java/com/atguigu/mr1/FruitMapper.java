package com.atguigu.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author xuzl
 * @create 2019-06-29 23:15
 */
public class FruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //构建put对象
        Put put = new Put(key.get());
        //遍历数据
        Cell[] cells = value.rawCells();
        for(Cell cell :cells){
            if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                put.add(cell);
            }
        }
        //写出去
        context.write(key,put);
    }
}
