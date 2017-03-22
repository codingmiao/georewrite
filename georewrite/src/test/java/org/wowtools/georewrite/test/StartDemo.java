package org.wowtools.georewrite.test;

import org.wowtools.georewrite.GwH2Manager;

public class StartDemo {

    //建立一个管理器，用来启动数据库，添加表等
    private static final GwH2Manager manager = new GwH2Manager(StartDemo.class);

    public static void main(String[] args) {
        manager.start();//根据conf/gwConfig.properties启动数据库
        /*
		 * 根据conf/tables/下的json文件加载表,
		 * 这些表会被geoserver识别，并以impl配置的实现类作为查询器去查询geoserver需要的数据
		 */
        manager.loadTables();
        System.out.println("数据库初始化完毕");
    }

}
