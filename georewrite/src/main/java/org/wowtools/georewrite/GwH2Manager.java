package org.wowtools.georewrite;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wowtools.common.utils.PropertiesReader;
import org.wowtools.common.utils.ResourcesReader;
import org.wowtools.georewrite.GeoSqlQueryer.ColumnDefinition;
import org.wowtools.h2.sqlrewriter.SqlRewriterManager;
import org.wowtools.h2.usrfun.UserFunctionManager;

import com.vividsolutions.jts.geom.Geometry;

public class GwH2Manager {
    private static final Logger logger = LoggerFactory.getLogger(GwH2Manager.class);
    private final Class<?> startClass;
    private final PropertiesReader pr;
    private JdbcConnectionPool connPool;

    /**
     * @param startClass 启动类
     */
    public GwH2Manager(Class<?> startClass) {
        this.startClass = startClass;
        pr = new PropertiesReader(startClass, "conf/gwConfig.properties");
    }

    /**
     * 启动h2数据库
     */
    public void start() {
        String dbName = pr.getString("dbName");
        String superDbUserName = pr.getString("superDbUserName");
        String superDbUserPwd = pr.getString("superDbUserPwd");
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create("jdbc:h2:mem:" + dbName + ";MVCC=TRUE",
                superDbUserName, superDbUserPwd);// 连接池
        connectionPool.setMaxConnections(pr.getInteger("maxConnections"));
        try {
            Server tcpServer = Server
                    .createTcpServer(new String[]{"-tcpPort", pr.getString("tcpPort"), "-tcpAllowOthers"});
            tcpServer.start();
        } catch (SQLException e) {
            throw new RuntimeException("启动tcp服务异常", e);
        }
        connPool = connectionPool;
        try {
            UserFunctionManager.register(connPool.getConnection(), GeoSqlRewriterFunction.class);
        } catch (SQLException e) {
            throw new RuntimeException("注册自定义函数异常", e);
        }
        SqlRewriterManager.add(new GeoSqlRewriter());
        logger.info("数据库启动完毕");
    }

    /**
     * 获取内存模式的数据库连接
     *
     * @return 数据库连接
     */
    public Connection getConnection() {
        try {
            return connPool.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("获取内存库连接异常", e);
        }
    }

    /**
     * 获取配置信息
     *
     * @return
     */
    public PropertiesReader getGwConfig() {
        return pr;
    }

    /**
     * 加载启动类相对路径下conf/tables下的所有json文件的配置信息，并注册为表
     *
     * @return 有多少表被加载
     */
    public int loadTables() {
        System.out.println(logger.getName());
        String rootPath = ResourcesReader.getRootPath(startClass);
        String strPath = rootPath + "/conf/tables";
        File tablesPath = new File(strPath);
        if (!tablesPath.exists()) {
            logger.warn("表路径{}不存在,跳过加载", strPath);
            return 0;
        }

        File[] files = tablesPath.listFiles((dir, name) -> {
            return name.indexOf(".json") > 0;
        });
        logger.info("发现表配置json文件数:{}", files.length);
        int n = 0;
        DataStore datastore = getDataStore();
        for (File f : files) {
            FileInputStream is = null;
            try {
                is = new FileInputStream(f);
                byte b[] = new byte[is.available()];
                is.read(b);
                String str = new String(b);
                JSONObject jo = new JSONObject(str);
                loadTable(jo, datastore);
                n++;
            } catch (Exception e) {
                logger.warn("加载表配置出错:" + f.getName(), e);
            } finally {
                if (null != is) {
                    try {
                        is.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        logger.info("加载数据表完成,成功加载数{}", n);
        return n;
    }

    /**
     * 加载表
     *
     * @param jo        查询器配置json
     * @param datastore gt数据源
     */
    @SuppressWarnings("unchecked")
    public void loadTable(JSONObject jo, DataStore datastore) {
        String tableName = jo.getString("tableName");
        JSONArray ja = jo.getJSONArray("columns");
        HashMap<String, ColumnDefinition> columnInfo = new HashMap<>(ja.length());
        int n = ja.length();
        for (int i = 0; i < n; i++) {
            JSONObject jc = ja.getJSONObject(i);
            String name = jc.getString("name");
            ColumnDefinition cd = new ColumnDefinition(name, jc.getInt("type"), jc.getInt("length"),
                    jc.getInt("precision"));
            columnInfo.put(name, cd);
        }
        GeoSqlQueryer query;
        try {
            String impl = jo.getString("impl");
            Class<? extends GeoSqlQueryer> clazz = (Class<? extends GeoSqlQueryer>) Class.forName(impl);
            Constructor<? extends GeoSqlQueryer> cons = clazz.getConstructor(String.class, HashMap.class, JSONObject.class);
            JSONObject initParam;
            if (jo.has("initParam")) {
                initParam = jo.getJSONObject("initParam");
            } else {
                initParam = null;
            }
            query = cons.newInstance(tableName, columnInfo, initParam);
        } catch (Exception e) {
            throw new RuntimeException("构造queryer实例出错", e);
        }
        loadTable(query, datastore);
    }

    /**
     * 加载表
     *
     * @param queryer   查询器
     * @param datastore gt数据源
     */
    public void loadTable(GeoSqlQueryer queryer, DataStore datastore) {
        // 在h2中建一张表，让geoserver能识别到它并作为数据源
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setNamespaceURI("http://acme.com");
        tb.setName(queryer.getTableName());
        // tb.srs("epsg:4326");//懒得搞geotools那套了，直接发布图层时强制指定一下
        queryer.getColumnInfo().forEach((name, cd) -> {
            Class<?> clazz;
            if (cd.type == ColumnDefinition.Type_Geometry) {
                clazz = Geometry.class;
            } else if (cd.type == Types.INTEGER) {
                clazz = Integer.class;
            } else if (cd.type == Types.NUMERIC) {
                clazz = Double.class;
            } else {
                clazz = String.class;
            }
            tb.add(name, clazz);
        });
        tb.add("TCODE", String.class);// 不存数据，纯粹拿来传递函数以及接收后渲染样式
        SimpleFeatureType type = tb.buildFeatureType();
        try {
            datastore.createSchema(type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 注册queryer
        GeoSqlRewriterFunction.registerGeoSqlQueryer(queryer.getTableName(), queryer);
    }

    /**
     * 获得一个geotools版的数据源，连接方式是通过tcp连接
     *
     * @return 数据源
     */
    public DataStore getDataStore() {
        Map<String, Object> params = new HashMap<>();
        params.put("dbtype", "h2");
        params.put("host", pr.getString("ip"));
        params.put("port", pr.getInteger("tcpPort"));
        params.put("database", "mem:" + pr.getString("dbName"));
        params.put("user", pr.getString("superDbUserName"));
        params.put("passwd", pr.getString("superDbUserPwd"));
        params.put("create spatial index", Boolean.TRUE);
        DataStore datastore;
        try {
            datastore = DataStoreFinder.getDataStore(params);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return datastore;
    }

}
