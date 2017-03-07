package org.wowtools.georewrite;

import org.wowtools.h2.sqlrewriter.SqlRewriter;

/**
 * 将geoserver发送给h2的sql进行改写</p>
 * sql格式形如:SELECT "fid","SHAPE" as "SHAPE" FROM "WWWW" WHERE  ("TCODE" = 'BS' AND "TCODE" IS NOT NULL  AND ST_Intersects("SHAPE",ST_GeomFromText('POLYGON ((82.96875 16.3916015625, 82.96875 33.6181640625, 117.0703125 33.6181640625, 117.0703125 16.3916015625, 82.96875 16.3916015625))', null)))
 * @author liuyu
 * @date 2016年12月26日
 */
public class GeoSqlRewriter implements SqlRewriter {

	@Override
	public boolean isConform(String sql) {
		return sql.indexOf("\"TCODE\" IS NOT NULL")>0;
	}

	@Override
	public String rewrite(String sql) {
		int b,e;
		b = sql.indexOf(" FROM ");
		//截取查询的字段
		String columnPart = sql.substring(7,b);
		e = sql.indexOf(" WHERE",b);
		String tableName = sql.substring(b+6,e);
		tableName = tableName.replace("\"", "");
		b = sql.indexOf("\"TCODE\" = '",e)+11;
		e = sql.indexOf("'",b);
		String fun = sql.substring(b,e);
		b = sql.indexOf("POLYGON ((");
		e = sql.indexOf("))",b)+2;
		String pg = sql.substring(b,e);
		StringBuilder sb = new StringBuilder();
		sb.append("select * ");
		sb.append("from GEOH2TABLE('").append(tableName).append("','");
		sb.append(columnPart).append("','");
		sb.append(fun).append("','");
		sb.append(pg).append("')");
		sql = sb.toString();
		return sql;
	}

}
