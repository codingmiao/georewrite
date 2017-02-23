# georewrite 快速实现geoserver的自定义数据源
email：[liuyu@wowtools.org][1]


----------


背景
==

加入我们需要从自定义的文件、远程服务等地方获取数据并在geoserver中发布为图层，或者是塞一套奇怪的查询业务到geoserver里，该怎么办呢？

geoserver提供了自定义数据源的方式，不过略复杂，基本流程是这样的：

 1. 创建一个类实现接口org.geotools.data.DataStoreFactorySpi，及相应的一套DataStore；
 2. 在目录META-INF/services/下增加文件org.geotools.data.DataStoreFactorySpi，内容为刚创建的类的完全类名；
 3. 将编译好的classes复制到<GeoServer install
path>\webapps\geoserver\WEB-INF\classes下，或者复制打包的jar文件到<GeoServer install path>\webapps\geoserver\WEB-INF\lib目录下。

但是这套弄下来，好像有点复杂哎-_-

那么，来点简单的吧，georewrite这个项目可以把原先写好的Java工程直接丢到geoserver中作数据源，你只需作一点小小的工作:

 1. 到geoserver官网下载h2数据库的插件并安装到你的geoserver；
 2. 把你原来Java工程的输出结果封装成SimpleResultSet对象，SimpleResultSet其实就是套了一层壳的ArrayList。


----------


快速开始
====

1、安装h2插件
------

 
到geoserver官网下载h2插件jar及h2的jdbc驱动jar，比如2.10版的:[http://geoserver.org/release/2.10.0/][2]，放到geoserver\WEB-INF\lib目录下，重启geoserver

![p1][3]
 
成功安装后，在geoserver中新建数据源中能看到H2的数据源:

![p2][4]

2、clone georewrite
------

 
clone下来后，执行mvn clean install即可。

如果不想使用maven，也可以直接下载[releases文件][7]，将其中的jar直接导入项目中即可。

3、编写自定义查询器:
------

编写一个类实现GeoSqlQueryer，主要代码如下：

    @Override
	public ResultSet query(Connection conn, String columnPart, String fun, String pg) throws SQLException {
		SimpleResultSet rs;
		//利用父类中的方法构造一个SimpleResultSet对象
		String[] columns = columnPart2columnArr(columnPart);
		rs = buildSimpleResultSetByColumns(columns);
		
		/**TODO
		 将原项目中的查询方法放在这，返回一个list，
		 并通过rs.addRow(obj[])方法将查询结果放到SimpleResultSet中
		**/
		
		return rs;
	}
	
这是一个完整的例子：[TestQueryer.java][8]

 4. 配置和启动
 参照[StartDemo.java][9]编写配置信息和启动类，然后运行启动类，自定义的数据源就配置完了，最后在geoserver中配置好数据源，即可使用：
![此处输入图片的描述][10]
(注意到图中CQL填写了 TCODE='HELLO',才触发了自定义的查询器，也可通过TCODE来传递更复杂的查询条件，来实现各种变态的业务逻辑)


  [1]: liuyu@wowtools.org
  [2]: http://geoserver.org/release/2.10.0/
  [3]: http://7xlvcv.com1.z0.glb.clouddn.com/6f1fa4c0-dd52-4a52-bbf2-e91143549761
  [4]: http://7xlvcv.com1.z0.glb.clouddn.com/405f47fa-81fb-44d0-ac3a-39a153dd8359
  [5]: https://github.com/codingmiao/catframe
  [6]: https://github.com/codingmiao/h2
  [7]: https://github.com/codingmiao/georewrite/releases
  [8]: https://github.com/codingmiao/georewrite/blob/master/georewrite/src/test/java/org/wowtools/georewrite/test/TestQueryer.java
  [9]: https://github.com/codingmiao/georewrite/blob/master/georewrite/src/test/java/org/wowtools/georewrite/test/StartDemo.java
  [10]: http://7xlvcv.com1.z0.glb.clouddn.com/1ef3364e-cff1-4772-b85b-974bcdfd4c6b
