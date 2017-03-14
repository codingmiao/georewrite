package org.wowtools.georewrite;

/**
 * @author liuyu
 * @date 2017/3/14
 */

/**
 * 列对象获取器,便于在循环对象前把 if ("SHAPE".equals(columnName) xxx这样的逻辑先做掉
 *
 * @author liuyu
 * @date 2016年12月28日
 */
@FunctionalInterface
public interface ObjGetter<T, CTX> {
    /**
     * 获取列值
     *
     * @param feature 查询得到的对象
     * @param fid     fid
     * @param tcode   TCODE
     * @param ctx     查询上下文，可将一些临时对象保存在ctx中，以免重复计算
     * @return 放入Result的row中的值
     */
    Object get(T feature, int fid, String tcode, CTX ctx);
}
