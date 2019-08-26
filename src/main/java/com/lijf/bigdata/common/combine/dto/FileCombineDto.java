package com.lijf.bigdata.common.combine.dto;

import lombok.Data;

/**
 * 文件合并数据
 */
@Data
public class FileCombineDto {
    private String fileDir;
    private String type;
    private Integer fileSize;
    private Integer partNum;

}
