package org.wowtools.georewrite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.wowtools.h2.usrfun.UserFunction;

/**
 * 本项目中用到的自定义函数
 *
 * @author liuyu
 * @date 2016年12月26日
 */
public class GeoSqlRewriterFunction {

    private static final HashMap<String, GeoSqlQueryer> queryerMap = new HashMap<>();

    /**
     * 处理GeoSqlRewriter重写的sql并返回结果集
     *
     * @param conn       h2数据库连接
     * @param tableName  实际查询的表名
     * @param columnPart 实际查询的列片段，形如"fid","SHAPE" as "SHAPE"
     * @param fun        TCODE带过来的查询函数
     * @param pg         查询范围多边形
     * @return 查询到的结果集
     * @throws SQLException h2内部执行错误时抛出
     * @see GeoSqlRewriter
     */
    @UserFunction("GEOH2TABLE")
    public static ResultSet geoH2Table(Connection conn, String tableName, String columnPart, String fun, String pg) throws SQLException {
        //获取查询器
        GeoSqlQueryer queryer = queryerMap.get(tableName);
        if (null == queryer) {
            throw new RuntimeException("未指定自定义表" + tableName + "的GeoSqlQueryer");
        }

        return queryer.query(conn, columnPart, fun, pg);
    }

    /**
     * 注册一个geosql查询器,将指定的表名转交给该查询器执行查询并返回结果集
     *
     * @see GeoSqlQueryer
     */
    public static void registerGeoSqlQueryer(String tableName, GeoSqlQueryer queryer) {
        synchronized (queryerMap) {
            queryerMap.put(tableName, queryer);
        }
    }
}
