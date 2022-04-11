package com.tiankx.flinksql.func;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author TIANKAIXUAN
 * @version 1.0.0
 * @date 2022/3/16 9:42
 */
public class ArrayToStrFunction extends ScalarFunction {
    public String eval(String[] arr){
        return StringUtils.join(arr, ",");
    }
}
