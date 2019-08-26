package com.lijf.bigdata.common.combine.core;



import com.lijf.bigdata.common.combine.CombineFile;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;


public class ParquetCombine extends CombineFile implements Serializable {
    @Override
    public long rowCount(Path path, Configuration conf) throws IOException {
        FileStatus[] inputFileStatuses = path.getFileSystem(conf).globStatus(path);
        List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatuses[0], false);
        List<BlockMetaData> blocks = footers.get(0).getParquetMetadata().getBlocks();
        long rowcount = blocks.get(0).getRowCount();
        return rowcount;
    }
    @Override
    public void combine(FileCombineDto dto, SparkSession spark) throws IOException {
        JavaRDD<String> parallelize = super.getFilePathsRdd(dto, spark);
        parallelize.foreachPartition(part -> {
            int partitionId = TaskContext.getPartitionId();
            UUID uuid = UUID.randomUUID();
            ExampleParquetWriter.Builder builderPart = null;
            ParquetWriter<Group> writerPart = null;
            while (part.hasNext()){
                String filePath = part.next();
                Path path = new Path(filePath);
                Configuration con = new Configuration();
                MessageType parquetSchemaByFile = getParquetSchemaByFile(con, path);
                String distPart = path.getParent().toUri().toString() +"_tmp"+ "/"
                        + "part"+partitionId+"-combine" + "_" + uuid + "." + "parquet";
                if(null == builderPart) {
                    builderPart = ExampleParquetWriter.builder(new Path(distPart)).withType(parquetSchemaByFile);
                }
                if(null == writerPart){
                    writerPart = builderPart.build();
                }
                GroupReadSupport readSupport = new GroupReadSupport();
                ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, path);
                ParquetReader<Group> build = reader.build();
                Group line = null;
                while ((line = build.read()) != null) {
                    writerPart.write(line);
                }
            }
            writerPart.close();
        });
        super.renameDir(dto);
    }



}
