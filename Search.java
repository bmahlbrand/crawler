import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Search {
 Connection connection;
    public Properties props;
    
    Search() {
    }

 public void readProperties() throws IOException {
        props = new Properties();
        FileInputStream in = new FileInputStream("database.properties");
        props.load(in);
        in.close();
    }
    
    public void openConnection() throws SQLException, IOException {
        String drivers = props.getProperty("jdbc.drivers");
        if (drivers != null) System.setProperty("jdbc.drivers", drivers);
        
        String url = props.getProperty("jdbc.url");
        String username = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");
        
        connection = DriverManager.getConnection( url, username, password);
    }
    

    public void searchDB(String query) throws SQLException, IOException {
        if (query == null)
         return;

     String[] arr = query.split(" ");

        openConnection();
        Statement stat = connection.createStatement();
        
        try {
          
        }
        catch (Exception e) {
        }
        if (arr[1] == null) {
         ResultSet result = stat.executeUpdate("SELECT * FROM crawler WHERE (word LIKE '"+arr[0]+"') or word LIKE GROUP BY urldid HAVING COUNT(urlid)>1");
     
         if (result.next()) {
          System.out.println("URL  already in DB");
         }
        } else if (arr[1] != null) {
         ResultSet result = stat.executeUpdate("SELECT * FROM crawler WHERE (word LIKE '"+arr[0]+"') or (word LIKE '"+arr[1]+"') GROUP BY urldid HAVING COUNT(urlid)>1");
        }
    }
}