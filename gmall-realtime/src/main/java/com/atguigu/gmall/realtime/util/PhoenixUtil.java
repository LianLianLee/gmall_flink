package com.atguigu.gmall.realtime.util;

import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    /**
     * 用于执行 Phoenix 建表语句或插入语句
     * @param sql 待执行的语句
     */
    public static void executeSQL(String sql) {

        DruidPooledConnection conn = null;
        try {
            conn = DruidDSUtil.getPhoenixConn();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("从 Druid 连接池获取连接对象异常");
        }

        PreparedStatement ps = null;
        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Phoenix 建表语句或插入语句执行异常");
        } finally {
            close(ps, conn);
        }
    }

    /**
     * 用于释放资源
     * @param ps 数据库操作对象
     * @param conn 连接对象
     */
    public static void close(PreparedStatement ps, Connection conn) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Phoenix 表查询方法
     * @param sql 查询数据的 SQL 语句
     * @param clz 返回的集合元素类型的 class 对象
     * @param <T> 返回的集合元素类型
     * @return 封装为 List<T> 的查询结果
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {

        DruidPooledConnection conn = null;
        try {
            conn = DruidDSUtil.getPhoenixConn();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            System.out.println("从 Druid 连接池获取连接对象异常");
        }

        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();

            /**处理结果集
             +-----+----------+
             | ID  | TM_NAME  |
             +-----+----------+
             | 17  | lzls     |
             | 18  | mm       |

             class TM{id,tm_name}
             */
            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()){
                //通过反射，创建对象，用于封装查询结果
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                resList.add(obj);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从phoenix数据库中查询数据发生异常了~~");
        } finally {
            //释放资源
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resList;
    }
}
