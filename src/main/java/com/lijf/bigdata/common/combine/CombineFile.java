package com.lijf.bigdata.common.combine;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;
import com.lijf.bigdata.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class CombineFile {


    public static Logger logger = LogManager.getLogger(CombineFile.class);

    abstract public void combine(FileCombineDto dto, SparkSession spark) throws IOException;
    abstract public long rowCount(Path path, Configuration conf) throws IOException;

    public void renameDir(FileCombineDto dto) throws IOException {
        String fileDir = dto.getFileDir();
        String distDir = fileDir + "_tmp";
        int size = HdfsUtils.listFiles(distDir).size();
        if(size>0){
            HdfsUtils.delete(fileDir);
            HdfsUtils.movefile(distDir,fileDir);
        }
    }

    public List<Path> getPaths(String src) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] fileStatuses = fs.listStatus(new Path(src));

        List<Path> fileList = new ArrayList<Path>();
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            fileList.add(path);
        }
        return fileList;
    }

    public JavaRDD<String> getFilePathsRdd(FileCombineDto dto, SparkSession spark) throws IOException {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        List<Path> paths = getPaths(dto.getFileDir());
        List<String> list = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Path path = paths.get(i);
            list.add(path.toUri().toString());
        }
        JavaRDD<String> parallelize = jsc.parallelize(list,dto.getPartNum());
        return parallelize;
    }


    public MessageType getParquetSchemaByFile(Configuration conf, Path  filePath) throws IOException {
        ParquetMetadata metaData;
        FileSystem fs = filePath.getFileSystem(conf);
        ParquetFileReader parquetFileReader = ParquetFileReader.open(conf,filePath);

        metaData = parquetFileReader.getFooter();
        MessageType schema = metaData.getFileMetaData().getSchema();
        logger.info("get parquet file schema:" + schema);
        return schema;

    }
}
