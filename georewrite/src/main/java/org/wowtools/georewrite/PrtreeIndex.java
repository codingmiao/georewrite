package org.wowtools.georewrite;

import java.awt.geom.Rectangle2D;
import java.util.ArrayList;
import java.util.Collection;

import org.khelekore.prtree.MBRConverter;
import org.khelekore.prtree.PRTree;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * 基于prtree构建的空间索引，由于prtree是一次性构建的，不支持动态修改，所以此类中也不包含任何用于修改的方法
 * 
 * @author liuyu
 * @date 2016年12月30日
 * @param <T>
 *            索引的对象类型，必须具备获取geometry的能力
 */
public class PrtreeIndex<T> {

	/**
	 * 查询结果访问器
	 * 
	 * @author liuyu
	 * @date 2016年12月30日
	 * @param <T>
	 *            索引的对象类型
	 */
	@FunctionalInterface
	public static interface ResultVister<T> {
		public void vist(T feature);
	}

	/**
	 * 根据传入的对象，获取其对应的geometry
	 * 
	 * @author liuyu
	 * @date 2016年12月30日
	 * @param <T>
	 *            索引的对象类型
	 */
	@FunctionalInterface
	public static interface GeometryBuilder<T> {
		public Geometry feature2Geometry(T feature);
	}

	/**
	 * 自定义的prtree的叶子节点
	 **/
	protected class PrtreeLeafNode extends Rectangle2D.Double {
		private static final long serialVersionUID = 1L;
		private T node;

		public PrtreeLeafNode(T node, double x, double y, double w, double h) {
			this.node = node;
			setRect(x, y, w, h);
		}

		@Override
		public boolean equals(Object obj) {
			@SuppressWarnings("unchecked")
			PrtreeLeafNode other = (PrtreeLeafNode) obj;
			return node.equals(other.node);
		}

		@Override
		public int hashCode() {
			return node.hashCode();
		}
	}

	/**
	 * 自定义的mbr转换器for PrtreeLeafNode
	 **/
	protected class NodeConverter implements MBRConverter<PrtreeLeafNode> {
		public int getDimensions() {
			return 2;
		}

		public double getMin(int axis, PrtreeLeafNode t) {
			return axis == 0 ? t.getMinX() : t.getMinY();
		}

		public double getMax(int axis, PrtreeLeafNode t) {
			return axis == 0 ? t.getMaxX() : t.getMaxY();
		}
	}

	private final PRTree<PrtreeLeafNode> tree;

	/**
	 * 
	 * @param features
	 *            需要索引的对象Collection
	 * @param geometryBuilder
	 *            描述如何从feature中获取geometry的实现类
	 */
	public PrtreeIndex(Collection<T> features, GeometryBuilder<T> geometryBuilder) {
		ArrayList<PrtreeLeafNode> leafNodes = new ArrayList<>(features.size());
		for (T feature : features) {
			// 得到几何对象外接矩形，进而构造一个PrtreeLeafNode节点
			Geometry geo = geometryBuilder.feature2Geometry(feature);
			if (null == geo) {
				continue;
			}
			Coordinate[] extent = geo.getEnvelope().getCoordinates();
			double xmin = extent[0].x;
			double ymin = extent[0].y;
			double w;
			double h;
			if (extent.length == 1) {// 点对象的外接矩形仅包含一个点
				w = 0;
				h = 0;
			} else if (extent.length == 5) {// 非点对象的外接矩形包含5个点,第三个点为(xmax,ymax)
				w = extent[2].x - xmin;
				h = extent[2].y - ymin;
			} else if (extent.length == 2) {// x或y相同的线
				w = extent[1].x - xmin;
				h = extent[1].y - ymin;
			} else if (extent.length == 0) {// empty
				continue;
			} else {
				throw new RuntimeException("未处理的extent:" + extent.length);
			}
			PrtreeLeafNode leafNode = new PrtreeLeafNode(feature, xmin, ymin, w, h);
			if (null != leafNode) {
				leafNodes.add(leafNode);
			}
		}

		int branchFactor = (int) (Math.cbrt(Math.sqrt(leafNodes.size())));// 设置树深度为6左右
		if (branchFactor < 4) {
			branchFactor = 4;
		}
		tree = new PRTree<PrtreeLeafNode>(new NodeConverter(), branchFactor);
		tree.load(leafNodes);
	}

	/**
	 * 范围查询
	 * 
	 * @param xmin
	 *            xmin
	 * @param ymin
	 *            ymin
	 * @param xmax
	 *            xmax
	 * @param ymax
	 *            ymax
	 * @param vister
	 *            vister
	 */
	public void queryEnvIntersect(double xmin, double ymin, double xmax, double ymax, ResultVister<T> vister) {
		Iterable<PrtreeLeafNode> iterable = tree.find(xmin, ymin, xmax, ymax);
		iterable.forEach((treeNode) -> {
			vister.vist(treeNode.node);
		});
	}
}
