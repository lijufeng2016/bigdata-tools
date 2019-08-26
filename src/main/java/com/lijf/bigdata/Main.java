package com.lijf.bigdata;

import com.lijf.bigdata.common.combine.CombineFile;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;
import com.lijf.bigdata.common.combine.factory.FileCombineFactory;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * @author lijf
 * @date 2019/8/21 16:03
 * @desc
 */

public class Main {
    public static void main(String[] args) throws IOException {

        SparkSession spark = getSparkSession();
        FileCombineDto fileCombineDto = new FileCombineDto();
        //最终的合并文件数
        fileCombineDto.setPartNum(8);
        // 要合并的目录
        fileCombineDto.setFileDir("/apps/hive/warehouse/mlg.db/tb_browser_user_action_20190823/dt=2019-08-21");
        // 文件格式
        fileCombineDto.setType("text");
        CombineFile combine = FileCombineFactory.getCombine(fileCombineDto);
        combine.combine(fileCombineDto, spark);
    }

    private static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .master("local[2]")
                .appName(Main.class.getSimpleName())
                .enableHiveSupport()
                .getOrCreate();
    }
}
