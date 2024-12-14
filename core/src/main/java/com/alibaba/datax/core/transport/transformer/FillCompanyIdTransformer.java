package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import groovy.lang.GroovyClassLoader;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.groovy.control.CompilationFailedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 为结算中心pg 缺失的company_id进行回填.
 * Created by wangminchao.
 */
public class FillCompanyIdTransformer extends Transformer {
    public FillCompanyIdTransformer() {
        setTransformerName("dx_fillCompanyId");
    }

    Map<String, String> meta;
    Long companyId;
    int index;
    @Override
    public Record evaluate(Record record, Object... paras) {

        Map<String, String> meta = record.getMeta();
        String schemaName = meta.get("schemaName");
        String columns = meta.get("columns");
        // 查找 company_id的下标
        columns.split(",");
        // 下标 - 1
        // record.setColumn(, new LongColumn(schemaName));
         return record;
        // if (meta == null){
        //     companyId = 1L;
        //     index = 1;
        // }
        // if (meta == null){
        //     meta = record.getMeta();
        //     String columns = meta.get("columns");
        //     String[] split = columns.split(",");
        //     index = Arrays.asList(split).indexOf("company_id");
        //     companyId = Long.parseLong(meta.get("schemaName"));
        // }
        //
        // record.setColumn(index + 1, new LongColumn(companyId));
        // // System.out.println("====>" + companyId + index  + record.toString());
        // return record;

        // Map<String, String> meta = record.getMeta();
        // String tableName = meta.get("tableName");
        // tableName = tableName.replaceAll("_\\d+$", "");
        // meta.put("tableName", tableName);
        // return record;
    }

}
