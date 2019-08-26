package com.lijf.bigdata.common.combine.core;

import com.lijf.bigdata.common.combine.CombineFile;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class TextCombine extends CombineFile implements Serializable {
    @Override
    public void combine(FileCombineDto dto, SparkSession spark) throws IOException {
        JavaRDD<String> parallelize = super.getFilePathsRdd(dto, spark);
        parallelize.foreachPartition(part ->{
            int partitionId = TaskContext.getPartitionId();
            UUID uuid = UUID.randomUUID();
            FileSystem hdfs = FileSystem.get(new Configuration());
            //创建文件输出流
            FSDataOutputStream fsOutStream = null;
            while (part.hasNext()){
                String filePath = part.next();
                Path path = new Path(filePath);
                String distPart = path.getParent().toUri().toString() +"_tmp"+ "/"
                        + "part"+partitionId+"-combine" + "_" + uuid + "." + "text";
                if(null == fsOutStream){
                    fsOutStream = hdfs.create(new Path(distPart), true);
                }
                //打开文件流
                FSDataInputStream inStream = hdfs.open(path);
                //读取文件内容到输出文件流
                IOUtils.copyBytes(inStream, fsOutStream, 4096, false);
                //关闭临时的输入流
                IOUtils.closeStream(inStream);
            }
            fsOutStream.close();
        });
        super.renameDir(dto);
    }

    @Override
    public long rowCount(Path path, Configuration conf) throws IOException {
        FileStatus[] inputFileStatuses = path.getFileSystem(conf).globStatus(path);
        List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatuses[0], false);
        List<BlockMetaData> blocks = footers.get(0).getParquetMetadata().getBlocks();
        long rowcount = blocks.get(0).getRowCount();
        return rowcount;
    }

}
