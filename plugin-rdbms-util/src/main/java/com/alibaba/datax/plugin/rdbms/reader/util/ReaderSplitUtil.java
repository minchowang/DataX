package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ReaderSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderSplitUtil.class);
    private static DataBaseType DATABASETYPE;

    public static boolean validateTableIsRegex(String tableName) {
        try {
            Pattern.compile(tableName);
            return true;
        } catch (PatternSyntaxException exception) {
            return false;
        }
    }

    private static String resetName(String name){

        char quotingChar;
        if (DATABASETYPE == DataBaseType.MySql){
            quotingChar = '`';
        }else if (DATABASETYPE == DataBaseType.PostgreSQL){
            quotingChar= '"';
        } else {
            throw new UnsupportedOperationException("Unsupported database type: " + DATABASETYPE);
        }

        name = name.replaceAll(quotingChar + "", "");

        return name;
    }
    private static String repeat(char quotingChar) {
        return new StringBuilder().append(quotingChar).append(quotingChar).toString();
    }
    private static String quote(String identifierPart, char quotingChar) {
        if (identifierPart == null) {
            return null;
        }

        if (identifierPart.isEmpty()) {
            return new StringBuilder().append(quotingChar).append(quotingChar).toString();
        }

        if (identifierPart.charAt(0) != quotingChar && identifierPart.charAt(identifierPart.length() - 1) != quotingChar) {
            identifierPart = identifierPart.replace(quotingChar + "", repeat(quotingChar));
            identifierPart = quotingChar + identifierPart + quotingChar;
        }

        return identifierPart;
    }
    public static String queryFullTableName(String catalogName, String schemaName, String tableName){
        StringBuilder quoted = new StringBuilder();

        char quotingChar;
        if (DATABASETYPE == DataBaseType.MySql){
            quotingChar = '`';
        }else if (DATABASETYPE == DataBaseType.PostgreSQL){
            quotingChar= '"';
        } else {
            throw new UnsupportedOperationException("Unsupported database type: " + DATABASETYPE);
        }

        if (catalogName != null && !catalogName.isEmpty()) {
            quoted.append(quote(catalogName, quotingChar)).append(".");
        }

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(quote(schemaName, quotingChar)).append(".");
        }

        quoted.append(quote(tableName, quotingChar));

        return quoted.toString();
    }

    public static String fullTableName(String catalogName, String schemaName, String tableName){
        StringBuilder quoted = new StringBuilder();


        if (catalogName != null && !catalogName.isEmpty()) {
            quoted.append(catalogName).append(".");
        }

        if (schemaName != null && !schemaName.isEmpty()) {
            quoted.append(schemaName).append(".");
        }

        quoted.append(tableName);

        return quoted.toString();
    }

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber, DataBaseType dataBaseType) {
        DATABASETYPE = dataBaseType;
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
        }

        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);

            Configuration tempSlice;

            Connection conn = DBUtil.getConnection(DATABASETYPE, jdbcUrl, sliceConfig.getString(Key.USERNAME), sliceConfig.getString(Key.PASSWORD));
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                // 处理正则表达式
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                tables = Collections.synchronizedList(new ArrayList<>(tables));
                List<String> tablesCopy = new ArrayList<>(tables);

                tables.clear();
                for (String table : tablesCopy) {
                    List<String> multiTables = new ArrayList<>();
                    boolean isRegex = validateTableIsRegex(table);
                    if (isRegex) {
                        DatabaseMetaData metaData = null;
                        Pattern pattern = Pattern.compile(table);
                        try {
                            ResultSet resultSet;
                            metaData = conn.getMetaData();
                            switch (DATABASETYPE) {
                                case PostgreSQL:
                                    resultSet= conn.createStatement().executeQuery("SELECT NULL AS TABLE_CAT, table_schema AS TABLE_SCHEM, table_name AS TABLE_NAME  FROM information_schema.tables");
                                    break;
                                case MySql:
                                    resultSet = metaData.getTables(null, null, null, new String[]{"VIEW", "TABLE"});
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unsupported database type: " + DATABASETYPE);
                            }
                            while (resultSet.next()) {
                                String catalogName = resultSet.getString("TABLE_CAT");
                                String schemaName = resultSet.getString("TABLE_SCHEM");
                                String tableName = resultSet.getString("TABLE_NAME");
                                if (pattern.matcher((fullTableName(catalogName, schemaName, tableName))).matches()) {
                                    multiTables.add(queryFullTableName(catalogName, schemaName, tableName));
                                }
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        if (!multiTables.isEmpty()){
                            tables.addAll(multiTables);
                        } else {
                            LOG.warn("table regex {} not match any table.", table);
                            throw new RuntimeException(String.format("table regex %s not match any table.", table));
                        }
                    }
                }


                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                String splitPk = originalSliceConfig.getString(Key.SPLIT_PK, null);

                //最终切分份数不一定等于 eachTableShouldSplittedNumber
                boolean needSplitTable = eachTableShouldSplittedNumber > 1
                        && StringUtils.isNotBlank(splitPk)
                        && tables.size() < 1024;
                if (needSplitTable) {
                    if (tables.size() == 1) {
                        //原来:如果是单表的，主键切分num=num*2+1
                        // splitPk is null这类的情况的数据量本身就比真实数据量少很多, 和channel大小比率关系时，不建议考虑
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 2 + 1;// 不应该加1导致长尾
                        
                        //考虑其他比率数字?(splitPk is null, 忽略此长尾)
                        //eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * 5;

                        //为避免导入hive小文件 默认基数为5，可以通过 splitFactor 配置基数
                        // 最终task数为(channel/tableNum)向上取整*splitFactor
                        Integer splitFactor = originalSliceConfig.getInt(Key.SPLIT_FACTOR, Constant.SPLIT_FACTOR);
                        eachTableShouldSplittedNumber = eachTableShouldSplittedNumber * splitFactor;
                    }
                    // 尝试对每个表，切分为eachTableShouldSplittedNumber 份
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        DatabaseMetaData metaData = null;
                        String pkName = null;
                        try {
                            metaData = conn.getMetaData();
                            String[] split = table.split("\\.");
                            ResultSet pkRs = metaData.getPrimaryKeys(null, resetName(split[0]), resetName(split[1]));
                            String defaultPK = "id";
                            while (pkRs.next()) {
                                pkName = pkRs.getString("COLUMN_NAME");
                                if (defaultPK.equals(pkName)) {
                                    break;
                                }
                            }
                            if (pkName == null) {
                                // 如果没有主键，那么就取第一个字段作为主键
                                ResultSet columns = metaData.getColumns(null, resetName(split[0]), resetName(split[1]), null);
                                while (columns.next()) {
                                    String columnName = columns.getString("COLUMN_NAME");
                                    pkName = columnName;
                                    break;
                                }
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        if (pkName != null) {
                            tempSlice.set(Key.SPLIT_PK, pkName);
                            List<Configuration> splittedSlices = SingleTableSplitUtil
                                    .splitSingleTable(tempSlice, eachTableShouldSplittedNumber);

                            splittedConfigs.addAll(splittedSlices);
                        } else {
                            String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                            tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                            splittedConfigs.add(tempSlice);
                        }

                    }
                } else {
                    for (String table : tables) {
                        tempSlice = sliceConfig.clone();
                        tempSlice.set(Key.TABLE, table);
                        String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column);
                        tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                        splittedConfigs.add(tempSlice);
                    }
                }
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL, String.class);

                // TODO 是否check 配置为多条语句？？
                for (String querySql : sqls) {
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.QUERY_SQL, querySql);
                    splittedConfigs.add(tempSlice);
                }
            }

            DBUtil.closeDBResources(null, null, conn);
        }

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();

        String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = queryConfig.getList(Constant.CONN_MARK, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++){
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<String>();
            List<String> splitPkQuerys = new ArrayList<String>();
            String connPath = String.format("connection[%d]",i);
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                for (String table : tables) {
                    querys.add(SingleTableSplitUtil.buildQuerySql(column,table,where));
                    if (splitPK != null && !splitPK.isEmpty()){
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(),table,where));
                    }
                }
                if (!splitPkQuerys.isEmpty()){
                    connConf.set(Key.SPLIT_PK_SQL,splitPkQuerys);
                }
                connConf.set(Key.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL,
                        String.class);
                for (String querySql : sqls) {
                    querys.add(querySql);
                }
                connConf.set(Key.QUERY_SQL,querys);
                queryConfig.set(connPath,connConf);
            }
        }
        return queryConfig;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }

}
