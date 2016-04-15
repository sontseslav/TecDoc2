/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tecdoc2;

/**
 *
 * @author coder007
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class MysqlHelper {
    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://10.0.2.2:3306/";
    private static final String DB_NAME = "tecdoc2015q1";
    private static final String DB_USER = "user";
    private static final String DB_PASSWORD = "12345";
    private static final String CHARSET_ENCODING = "utf-8";
    private Connection connection = null;
 
    public MysqlHelper() {
        try {
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_URL + DB_NAME 
                    + "?characterEncoding="+CHARSET_ENCODING+"&useUnicode=true", 
                    DB_USER, DB_PASSWORD);
            
            String sqlCharset = "SET NAMES utf8 COLLATE utf8_general_ci";
            Statement st = connection.createStatement();
            st.executeQuery(sqlCharset);
            st.executeQuery("SET CHARACTER SET utf8");
            System.out.println("Connected");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public Connection getConnection() {
        return connection;
    }
    
}
