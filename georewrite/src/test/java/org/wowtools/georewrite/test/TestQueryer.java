package org.wowtools.georewrite.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Random;

import org.h2.tools.SimpleResultSet;
import org.wowtools.georewrite.GeoSqlQueryer;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKBWriter;

/**
 * 一个查询器的例子
 * </p>
 * 初始化时，在范围[100,20,120,30]之间生成了10,000个点，查询时返回它们
 * 
 * @author liuyu
 * @date 2016年12月27日
 */
public class TestQueryer extends GeoSqlQueryer {
	private static final int num = 10000;

	private static class MyFeature {
		int id;// 字段中不要带fid，否则geoserver会在查询时重命名一个fid_1之类的出来，很麻烦
		String name;
		double value;
		Point shape;
		byte[] wkb;
	}

	private static final MyFeature[] features = new MyFeature[num];
	static {
		// 初始化时搞一批测试数据
		Random r = new Random(233);
		GeometryFactory gf = new GeometryFactory();
		WKBWriter wr = new WKBWriter();
		for (int i = 0; i < num; i++) {
			MyFeature f = new MyFeature();
			f.id = i;
			f.name = "point-" + i;
			f.value = r.nextDouble();
			Coordinate coordinate = new Coordinate(100 + r.nextDouble() * 20, 20 + r.nextDouble() * 10);
			Point pt = gf.createPoint(coordinate);
			f.shape = pt;
			f.wkb = wr.write(pt);
			features[i] = f;
		}
	}

	public TestQueryer(String tableName, HashMap<String, ColumnDefinition> columnInfo) {
		super(tableName, columnInfo);
	}

	@Override
	public ResultSet query(Connection conn, String columnPart, String fun, String pg) throws SQLException {
		SimpleResultSet rs;
		// 利用父类中的方法构造一个SimpleResultSet对象
		String[] columns = columnPart2columnArr(columnPart);
		rs = buildSimpleResultSetByColumns(columns);

		// (可选操作)构建一个对象获取器数组，提前处理if ("SHAPE".equals(columnName)之类的判断
		ObjGetter<MyFeature>[] objGetters = feature2Row(columns);

		// 查询features中在范围内的数据并返回(实际运用中，我们应该建立一个索引，而非像本例子中一样遍历)
		Geometry extent = pg2Extent(pg);
		for (MyFeature f : features) {
			if (extent.intersects(f.shape)) {
				Object[] row = new Object[objGetters.length];
				for (int i = 0; i < objGetters.length; i++) {
					row[i] = objGetters[i].get(f, -1, fun);
				}
				rs.addRow(row);
			}
		}
		return rs;
	}

	private ObjGetter<MyFeature>[] feature2Row(String[] columns) {
		@SuppressWarnings("unchecked")
		ObjGetter<MyFeature>[] objGetters = new ObjGetter[columns.length];
		for (int i = 0; i < columns.length; i++) {
			ObjGetter<MyFeature> getter;
			String columnName = columns[i];
			if ("SHAPE".equals(columnName)) {
				getter = (f, fid, tcode) -> {
					return f.wkb;
				};
			} else if ("VALUE".equals(columnName)) {
				getter = (f, fid, tcode) -> {
					return f.value;
				};
			} else if ("NAME".equals(columnName)) {
				getter = (f, fid, tcode) -> {
					return f.name;
				};
			} else if ("TCODE".equals(columnName)) {
				getter = (f, fid, tcode) -> {
					return tcode;
				};
			} else {
				getter = (f, fid, tcode) -> {
					return f.id;
				};
			}
			objGetters[i] = getter;
		}
		return objGetters;
	}

}
