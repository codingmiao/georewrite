package org.wowtools.georewrite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

import org.h2.tools.SimpleResultSet;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * geosql查询器
 *
 * @author liuyu
 * @date 2016年12月27日
 * @see GeoSqlRewriterFunction
 */
public abstract class GeoSqlQueryer {
    private static final Logger logger = LoggerFactory.getLogger(GeoSqlQueryer.class);

    /**
     * 结果集列定义
     *
     * @author liuyu
     * @date 2016年12月27日
     */
    public static class ColumnDefinition {
        public static final int Type_Geometry = 10000;
        /**
         * 名称
         */
        public final String name;
        /**
         * 类型， 10000 表示geometry
         *
         * @see Types
         */
        public final int type;
        /**
         * 长度
         */
        public final int length;
        /**
         * 精度
         */
        public final int precision;

        /**
         * @param name      名称
         * @param type      类型， 10000 表示geometry
         * @param length    长度
         * @param precision 精度
         * @see Types
         */
        public ColumnDefinition(String name, int type, int length, int precision) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.precision = precision;
        }
    }

    private final HashMap<String, ColumnDefinition> columnInfo;
    private final String tableName;

    /**
     * 若需要使用buildSimpleResultSetByColumns方法来构造一个SimpleResultSet对象作为返回，
     * 需要通过此构造方法，显式地指定可能会查询到的字段及其类型
     *
     * @param tableName  表名
     * @param columnInfo 列信息map，key为列名，value为java.sql.Types中所定义的列类型
     * @param initParam  初始化时的需要的自定义参数
     */
    public GeoSqlQueryer(String tableName, HashMap<String, ColumnDefinition> columnInfo, JSONObject initParam) {
        this.columnInfo = columnInfo;
        this.tableName = tableName;
    }

    /**
     * 若query方法的实现中不使用buildSimpleResultSetByColumns方法来构造一个SimpleResultSet对象作为返回，
     * 则无需指定可能会查询到的字段及其类型，使用此构造方法即可
     *
     * @param tableName 表名
     * @param initParam 初始化时的需要的自定义参数
     */
    public GeoSqlQueryer(String tableName, JSONObject initParam) {
        this.columnInfo = null;
        this.tableName = tableName;
    }

    /**
     * 依据查询条件，返回查询结果
     *
     * @param conn       h2数据库连接
     * @param columnPart 实际查询的列片段，形如"fid","SHAPE" as "SHAPE"
     * @param fun        TCODE带过来的查询函数
     * @param pg         查询范围多边形
     * @return 查询到的结果集
     * @throws SQLException h2内部执行错误时抛出
     */
    public abstract ResultSet query(Connection conn, String columnPart, String fun, String pg) throws SQLException;

    /**
     * 将columnPart解析为数组
     *
     * @param columnPart 实际查询的列片段，形如"fid","SHAPE" as "SHAPE"
     * @return 数组格式的字段名
     */
    protected String[] columnPart2columnArr(String columnPart) {
        String[] columns = columnPart.split(",");
        for (int i = 0; i < columns.length; i++) {
            String c = columns[i];
            c = c.replace("\"", "");
            int bAs = c.indexOf(" as ");
            if (bAs > 0) {
                c = c.substring(bAs + 4);
            }
            columns[i] = c;
        }
        return columns;
    }

    /**
     * 根据需要查询的列名，构造一个SimpleResultSet对象
     *
     * @param columns 列名
     * @return SimpleResultSet对象
     * @throws SQLException 构造SimpleResultSet异常时抛出
     */
    @SuppressWarnings("resource")
    protected SimpleResultSet buildSimpleResultSetByColumns(String[] columns) throws SQLException {
        if (null == columnInfo) {
            throw new RuntimeException("未通过 GeoSqlQueryer(HashMap<String, Integer> columnInfo)指定列类型");
        }
        SimpleResultSet rs = new SimpleResultSet();
        try {
            for (String c : columns) {
                ColumnDefinition cd = columnInfo.get(c);
                if (null == cd) {
                    if ("fid".equals(c)) {
                        rs.addColumn("fid", Types.INTEGER, 10, 0);
                        continue;
                    } else if ("TCODE".equals(c)) {
                        rs.addColumn("TCODE", Types.VARCHAR, 10, 0);
                        continue;
                    } else {
                        throw new RuntimeException("未指定的列:" + tableName + "." + c);
                    }
                }
                int type = cd.type;
                if (type == ColumnDefinition.Type_Geometry) {
                    type = Types.VARBINARY;
                }
                rs.addColumn(c, type, cd.length, cd.precision);
            }
        } catch (Exception e) {
            logger.error("buildSimpleResultSetByColumns 出错", e);
            throw new RuntimeException("buildSimpleResultSetByColumns 出错", e);
        }
        return rs;
    }

    /**
     * 将输入的范围wkt转为Geometry对象
     *
     * @param pg 将输入的范围wkt
     * @return 范围Geometry
     */
    protected Geometry pg2Extent(String pg) {
        try {
            WKTReader r = new WKTReader();
            return r.read(pg);
        } catch (ParseException e) {
            throw new RuntimeException("解析输入范围错误:" + pg, e);
        }
    }

    /**
     * 将输入的范围wkt转为[xmin,ymin,xmax,ymax]
     *
     * @param pg 将输入的范围wkt
     * @return [xmin, ymin, xmax, ymax]
     */
    protected double[] pg2ExtentCoord(Geometry geo) {
        // 得到几何对象外接矩形，进而构造一个PrtreeLeafNode节点
        Coordinate[] extent = geo.getEnvelope().getCoordinates();
        double xmin = extent[0].x;
        double ymin = extent[0].y;
        double xmax = extent[2].x;
        double ymax = extent[2].y;
        return new double[]{xmin, ymin, xmax, ymax};
    }

    /**
     * 将输入的范围wkt转为[xmin,ymin,xmax,ymax]
     *
     * @param pg 将输入的范围wkt
     * @return [xmin, ymin, xmax, ymax]
     */
    protected double[] pg2ExtentCoord(String pg) {
        try {
            WKTReader r = new WKTReader();
            Geometry geo = r.read(pg);
            return pg2ExtentCoord(geo);
        } catch (ParseException e) {
            throw new RuntimeException("解析输入范围错误:" + pg, e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public HashMap<String, ColumnDefinition> getColumnInfo() {
        return columnInfo;
    }

}
