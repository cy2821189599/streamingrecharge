package util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


public class C3p0Util {
    public static ComboPooledDataSource cpds = null;

    /**
     * 创建c3p0连接池
     */
    private static void createConnectionPool() {
        Properties prop = new Properties();
        try {
            prop.load( C3p0Util.class.getClassLoader().getResourceAsStream( "c3p0.properties" ) );
            System.out.println( prop );
        } catch (IOException e) {
            e.printStackTrace();
        }
        cpds = new ComboPooledDataSource( true );
        try {
            cpds.setJdbcUrl( prop.getProperty( "jdbcUrl" ) );
            cpds.setDriverClass( prop.getProperty( "driverClass" ) );
            cpds.setUser( prop.getProperty( "username" ) );
            cpds.setPassword( prop.getProperty( "password" ) );
            cpds.setMinPoolSize( new Integer( prop.getProperty( "minPoolSize" ) ) );
            cpds.setMaxPoolSize( new Integer( prop.getProperty( "maxPoolSize" ) ) );
            cpds.setInitialPoolSize( new Integer( prop.getProperty( "initialPoolSize" ) ) );
            cpds.setMaxIdleTime( new Integer( prop.getProperty( "maxIdleTime" ) ) );
            cpds.setAcquireIncrement( new Integer( prop.getProperty( "acquireIncrement" ) ) );

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static synchronized void connectionPoolInit() {
        if ( cpds == null ) {
            createConnectionPool();
        }
    }

    /**
     * 获取连接池
     *
     * @return 返回一个连接
     */
    public static Connection getConnection() {
        try {
            if ( cpds == null ) {
                connectionPoolInit();
            }
            Connection conn = cpds.getConnection();
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭连接
     *
     * @param conn Connection
     * @param pst  PreparedStatement
     * @param rs   ResultSet
     */
    public static void close(Connection conn, PreparedStatement pst, ResultSet rs) {
        if ( rs != null ) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if ( pst != null ) {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if ( conn != null ) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection conn, PreparedStatement pst) {
        if ( pst != null ) {
            try {
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if ( conn != null ) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection conn) {
        if ( conn != null ) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
