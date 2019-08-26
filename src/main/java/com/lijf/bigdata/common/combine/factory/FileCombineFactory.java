package com.lijf.bigdata.common.combine.factory;


import com.lijf.bigdata.common.combine.CombineFile;
import com.lijf.bigdata.common.combine.core.OrcCombine;
import com.lijf.bigdata.common.combine.core.ParquetCombine;
import com.lijf.bigdata.common.combine.core.TextCombine;
import com.lijf.bigdata.common.combine.dto.FileCombineDto;

public class FileCombineFactory {


    public static CombineFile getCombine(FileCombineDto dto){
        if("orc".equals(dto.getType())){
            return new OrcCombine();
        }else if("parquet".equals(dto.getType())){
            return new ParquetCombine();
        }else if("text".equals(dto.getType())){
            return new TextCombine();
        }
        return null;
    }
}
