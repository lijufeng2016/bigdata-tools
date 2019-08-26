package com.lijf.bigdata.common.combine.core;

import com.lijf.bigdata.common.combine.CombineFile;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.UUID;

public class OrcCombine extends CombineFile {

    @Override
    public long rowCount(Path path,Configuration conf) throws IOException {
        Reader reader = OrcFile.createReader(path,
                OrcFile.readerOptions(conf));
        long numberOfRows = reader.getNumberOfRows();
        return numberOfRows;
    }


    @Override
    public void combine(FileCombineDto dto, SparkSession spark) throws IOException {
        // TODO 待测
        JavaRDD<String> parallelize = super.getFilePathsRdd(dto, spark);
        parallelize.foreachPartition(part -> {
            Configuration conf = new Configuration();
            int partitionId = TaskContext.getPartitionId();
            UUID uuid = UUID.randomUUID();
            Writer writer = null;

            while (part.hasNext()){
                Path path = new Path(part.next());
                String distPart = path.getParent().toUri().toString() +"_tmp"+ "/"
                        + "part"+partitionId+"-combine" + "_" + uuid + "." + "orc";
                Reader reader = OrcFile.createReader(path,
                        OrcFile.readerOptions(conf));
                RecordReader rows = reader.rows();
                TypeDescription schema = reader.getSchema();
                VectorizedRowBatch batch = schema.createRowBatch();
                while (rows.nextBatch(batch)) {
                    if(writer==null){
                        writer = OrcFile.createWriter(new Path(distPart),
                                OrcFile.writerOptions(conf)
                                        .setSchema(schema));
                    }
                    if (batch.size != 0) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
                rows.close();
            }
            writer.close();
        });
        super.renameDir(dto);
    }
}
