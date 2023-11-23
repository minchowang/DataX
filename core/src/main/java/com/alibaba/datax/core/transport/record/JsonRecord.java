package com.alibaba.datax.core.transport.record;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.util.ClassSize;
import com.alibaba.fastjson2.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// 这是一个json record
public class JsonRecord implements Record {

    private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

    private JSONObject jsonObject;
    private List<String> columnsName;
    private int index;

    private int byteSize;

    // 首先是Record本身需要的内存
    private int memorySize = ClassSize.DefaultRecordHead;

    private Map<String, String> meta;

    public JsonRecord() {
        this.jsonObject = new JSONObject();
        this.columnsName = new ArrayList<>();
        this.index = 0;
    }

    public void addColumnName(int index, String columnName){
        columnsName.add(index-1, columnName);
    }

    @Override
    public void addColumn(Column column) {
        jsonObject.put(columnsName.get(index++), column.asString());
        byteSize += column.getByteSize();
    }

    @Override
    public void setColumn(int i, Column column) {

    }

    @Override
    public Column getColumn(int i) {
        return new StringColumn(jsonObject.toString());
    }

    @Override
    public int getColumnNumber() {
        return jsonObject.size();
    }

    @Override
    public int getByteSize() {
        return byteSize;
    }

    @Override
    public int getMemorySize() {
        return 0;
    }

    @Override
    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    @Override
    public Map<String, String> getMeta() {
        return this.meta;
    }
}
