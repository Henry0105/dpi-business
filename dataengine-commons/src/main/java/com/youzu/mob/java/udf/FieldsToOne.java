package com.youzu.mob.java.udf;

import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

public class FieldsToOne extends UDF {

    private List<String> fieldsMapping = null;

    public String evaluate(String ... strs){
        if(null == fieldsMapping){
            fieldsMapping = Arrays.stream(strs[0].split("\\|")).map(String::trim).collect(Collectors.toList());
        }
        StringBuilder nameBuf = new StringBuilder();
        StringBuilder valueBuf = new StringBuilder();
        for (int i = 1; i < strs.length; i++) {
            String vn = fieldsMapping.get(i-1);
            String v = strs[i];
            if(isDouble(v)){
                v = new BigDecimal(v).setScale(4, RoundingMode.HALF_UP).toString();
            }
            //非null，为数值，不为0
            if((isNumber(v) && Double.valueOf(v) != 0) || (StringUtils.isNotBlank(v) && !isNumber(v))){
                if(nameBuf.length() == 0){
                    nameBuf.append(vn);
                    valueBuf.append(v);
                }else{
                    nameBuf.append(",").append(vn);
                    valueBuf.append(",").append(v);
                }
            }
        }
        if(nameBuf.length() > 0 && valueBuf.length() > 0){
            return nameBuf.append("=").append(valueBuf).toString();
        }else{
            return null;
        }
    }

    private static boolean isDouble(String value){
        try{
            if(value.contains(".")){
                Double.valueOf(value);
                return true;
            }else{
                return false;
            }
        }catch (Exception e){
            return false;
        }
    }

    private static boolean isNumber(String value){
        try{
            Double.valueOf(value);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    public static void main(String[] args){

        new FieldsToOne().evaluate("'F1001|F1002|F1003|F1004|F1005|F1006|F1007|F1008|F1009|F1010|F1011|F0012|F1013|F1014|F1015|F1016|F1017|F1018|F1019|F1020|F1021|F1022|F1023|F1024|F1025|F1026|F1027|F1028|F1029|F1030|F1031|F1032|F1033|F1034|F1035|F1036|F1037|F1038|F1039'," +
                "                borrowing1, bank2, investment3, finaces4, securities5, insurance6, total7, insurance8, borrowing9, bank10,\n" +
                "                investment11, securities12, finaces13, total14, insurance15, borrowing16, bank17, investment18, securities19, finaces20,\n" +
                "                total21, insurance22, borrowing23, bank24, investment25, securities26, finaces27, total28, insurance_slope29,\n" +
                "                borrowing_slope30, bank_slope31, investment_slope32, securities_slope33, finaces_slope34, total_slope35,\n" +
                "                coalesce(f.cnt, ''), coalesce(g.cnt, ''), coalesce(h.cnt, ''), coalesce(i.cnt, '')".split(","));
    }
}
