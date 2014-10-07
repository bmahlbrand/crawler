import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jsoup.UnsupportedMimeTypeException;

public class Crawler {
    Connection connection;
    int urlID;
    int occurence;
    public Properties props;
    Queue<String> queue = new Queue<String>();

    Crawler() {
        urlID = 0;
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
    
    public void createDB() throws SQLException, IOException {
        openConnection();
        
        Statement stat = connection.createStatement();
        
        // Delete the table first if any
        try {
            // stat.executeUpdate("DROP TABLE URLS");
            // stat.executeUpdate("DROP TABLE WORDS");
        }
        catch (Exception e) {
        }
        
        // Create the table
        stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(1500))");
        stat.executeUpdate("CREATE TABLE WORDS (urlid INT, word VARCHAR(40), occurence INT)");
       // stat.executeUpdate("ALTER TABLE WORDS ADD CONSTRAINT UQ_urlid_word UNIQUE(urlid, word)");
    }
    
    public boolean urlInDB(String urlFound) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");

        
        if (result.next()) {
            System.out.println("URL "+urlFound+" already in DB");
            
            return true;
        }
        // System.out.println("URL "+urlFound+" not yet in DB");
        return false;
    }
    
    public boolean wordInDB(String word) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        // ResultSet result = stat.executeQuery("SELECT * FROM words WHERE word LIKE '"+word+"' GROUP BY word, urlid HAVING count(urlid) > 1");
        ResultSet result = stat.executeQuery("SELECT urlid, word, count(urlid) FROM words WHERE word = '"+word+"' AND urlid GROUP BY word, urlid HAVING count(urlid) > 1");
        //ResultSet result = stat.executeQuery("SELECT * FROM words WHERE word LIKE '"+word+"' GROUP BY word, urlid HAVING count(urlid) > 1");
            //"SELECT *, COUNT(urlid) as times FROM words WHERE word like '"+word+"' AND '"+urlID+"' group by word, urlid having times = 1");
        
        if (result.next()) {
         //   stat.executeUpdate("UPDATE words SET occurence=occurence+1 WHERE word = '"+word+"'");
            System.out.println("WORD "+word+" already in DB");
            
            return true;
        }
        // System.out.println("URL "+urlFound+" not yet in DB");
        return false;
    }

    public void insertURLInDB(String url, String description) throws SQLException, IOException {
        // if (!url.contains("\'")) {
        try {
            String query = "INSERT INTO urls (urlid, url, description) VALUES (?,?,?)";
            PreparedStatement stat = connection.prepareStatement(query);
            
            //System.out.println("Executing "+query);
            stat.setInt(1, urlID);
            stat.setString(2, url);
            stat.setString(3, description);
            stat.executeUpdate();
            urlID++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
       
        // }
        
    }
    public void insertWORDInDB(String word) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        String query = "INSERT INTO words VALUES ('"+urlID+"','"+word+"', '"+occurence+"') ON DUPLICATE KEY UPDATE occurence = occurence+1";//,'"+description+"')";
        stat.executeUpdate(query);
    }
    
    void addWordsToDB(String url, String description, int urlID) {
        
        Pattern p = Pattern.compile("[^a-zA-Z0-9]");

        String[] arr = description.split(" ");
        for (String word : arr) {
            try {
                if(!urlInDB(url)) {
                    boolean hasSpecialChar = p.matcher(word).find();
                    if (!hasSpecialChar) {
                        // if (!wordInDB(word)) {
                            // System.out.println(word);
                            insertWORDInDB(word.toLowerCase());
                        // } else {
                            //increment word counter
                        // }
                    }                    
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String makeAbsoluteURL(String url, String parentURL) {
        if (url.indexOf(":")<0) {
            // the protocol part is already there.
            return url;
        }
        
        if (url.length() > 0 && url.charAt(0) == '/') {
            // It starts with '/'. Add only host part.
            int posHost = url.indexOf("://");
            if (posHost <0) {
                return url;
            }
            int posAfterHost = url.indexOf("/", posHost+3);
            if (posAfterHost < 0) {
                posAfterHost = url.length();
            }
            String hostPart = url.substring(0, posAfterHost);
            return hostPart + "/" + url;
        } 
        
        // URL start with a char different than "/"
        int pos = parentURL.lastIndexOf("/");
        int posHost = parentURL.indexOf("://");
        if (posHost <0) {
            return url;
        }
        return null;
    }
    
    public void fetchURL(String urlScanned, String description) {
        try {
            URL url = new URL(urlScanned);
            System.out.println("urlscanned="+urlScanned+" url.path="+url.getPath());
            
            // open reader for URL
            InputStreamReader in = 
                new InputStreamReader(url.openStream());
            
            // read contents into string builder
            StringBuilder input = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                input.append((char) ch);
            }
            
            // search for all occurrences of pattern
            String patternString =  "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";
            Pattern pattern =    
                Pattern.compile(patternString, 
                                Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(input);
            
            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                String match = input.substring(start, end);
                String urlFound = matcher.group(1);
                System.out.println(urlFound);
                
                // Check if it is already in the database
                if (!urlInDB(urlFound)) { // && urlFound.contains("cs.purdue")
                    addWordsToDB(urlFound, description, urlID);
                    insertURLInDB(urlFound, description);//, "input");
                } else {
                    //increment occurence
                }   
                
                //System.out.println(match);
            }
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    void startCrawl() {
        //Clear database
        
        int NextURLID = 0;
        int NextURLIDScanned = 0;
        // Document doc = Jsoup.connect(url).get();
        //        Open the database
        //        for every url in url-list {
        //            urlid = NextURLID;
        //        insertURLInDB(url);//  to the URL table
        //        NextURLID++;
        
        //       }
    }

    String getDescription(String url) {
        String description = createDoc(url).select("body").text();
        if (description.length() > 1500) {
           description = description.substring(0, 1499);
        }    
        return description;
    }
    

    void addToQueue(Element links) {
        
    }
    void getLinks(Document doc) {
        String next = null;
        int marker = 0, count = 0;
        try {
            while (count < 6000) {

                if (doc == null) {
                    // continue;
                }
                Elements links = doc.select("a");
                
                for (Element link : links) {
                    String url = link.absUrl("href");

                    if(marker == 0  && !url.equals("http://purdue.edu") && link.absUrl("href") != null && url.contains("cs.purdue") && !url.contains("ftp")
                        && !url.contains("#") && !url.contains("?") && !url.contains("=")  && !url.contains("php")) {
                        
                        if (createDoc(link.absUrl("href")).select("a") != null && !urlInDB(url) && url.endsWith(".html")) {
                            next = url;
                            marker = 1;
                        }
                    }

                    // url = makeAbsoluteURL(url, "http://www.cs.purdue.edu/");
                    
                    if(link.absUrl("href") != null && url.contains("cs.purdue") && !url.contains("ftp")
                        && !url.contains("#") && !url.contains("?") && !url.contains("=") && !url.contains("php")) {
                        
                        doc = createDoc(url);
                        String description = doc.select("body").text();// "p"
                        
                        if (description != null && description.length() > 1500) {
                           description = description.substring(0, 1499);
                        }  
                        
                          // Check if it is already in the database
                        if (!urlInDB(url) && url.contains("cs.purdue")) { // && urlFound.contains("cs.purdue")
                            count++;

                            addWordsToDB(url, description, urlID);
                            insertURLInDB(url, description);//, "input");

                            if (doc.select("a") != null) {
                                Elements buffer = doc.select("a");
                                boolean shouldTraverse = false;
                                for (Element bufferedLink : buffer) {
                                    String bufferedUrl = bufferedLink.absUrl("href");
                                    if (!urlInDB(bufferedUrl) && createDoc(bufferedLink.absUrl("href")).select("a") != null) {
                                        shouldTraverse = true;
                                        break;
                                    }
                                }
                                if (shouldTraverse)
                                    getLinks(doc);
                            }
                        } else {
                            //increment occurence
                        }   

                    }
                }
                System.out.println("----------------");
                System.out.println(next);
                doc = createDoc(next);
                marker = 0;
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    
    Document createDoc(String url) {
        try {
            if (!url.contains("slider")) {
                System.out.println(url);
                return Jsoup.connect(url).get(); 
            } else {
                return null;
            }
            
        } catch (SocketTimeoutException ex) {
            try {
                return Jsoup.parse(new URL(url), 3000);
            } catch (IOException e) {
                return null;
            }
        } catch (UnsupportedMimeTypeException ex) {
            return new Document("No Links");
        } catch (IOException ex) {
            return null;
        }
    }

    void crawl(String url) {
        String html = null;
        Document doc = null;
        String description = null;
        Elements links = null;
        //String description;
        //   while (NextURLIDScanned < NextURLID) {
        //     urlIndex = NextURLIDScanned;
        //     Fetch the url1 entry in urlIndex 
        while (urlID < 5000) {

        try {
            //if (urlID == 0) {
                html = Jsoup.connect("http://www.cs.purdue.edu").get().html(); 
                doc = Jsoup.parse(html, "UTF-8");
                links = doc.select("a[href]");//get all links
                getLinks(doc);
            //}

            doc = createDoc(html);
            getLinks(doc);
            for (Iterator iterator = links.iterator(); iterator.hasNext();) {
                
                Element element = (Element) iterator.next();
                html = Jsoup.connect(element.attr("abs:href").toString()).get().html();//errors spew from here
                doc = createDoc(html);
                
                fetchURL(element.attr("href"), description);
                //System.out.println(element.attr("abs:href"));
                
                makeAbsoluteURL(element.toString(), html);
                
                
                //
               // System.out.println(description);
               // addWordsToDB(element.attr("abs:href"), description, urlID);
            
                
               // html = element.attr("href"); 
                //insertURLInDB(html, getDescription(html));
                System.out.println(element.attr("abs:href"));
            }

            } catch(Exception e) {
                e.printStackTrace();
            }
       // links = doc.select("a[href]");
        } 
    }
        //     NextURLIDScanned++;
        //     Get the first 100 characters or less of the document from url1 without tags. Add this 
        //      description to the URL record in the URL table.
        
        //     For each url2 in the the links in the anchor tags of this document {
        //      fetch the url2 in the link 
        //      if it is not text/html continue;
        
        //      if (NextURLID < MaxURLs && url2 is not already in URL table) {
        //       put (NextURLID, url2) in the URL table
        //      NextURLID++;
        //      }
        //     }
        
        //     Get the document in url1 without tags
        //      for each different word in the document {
        //           In Word Table create a new (word, URLID) if the entry does not exist.     
        //      }  
        //     }//while
    
    public static void main(String[] args)
    {   
        String html = null;
        Document doc = null;
        String description = null;
        Crawler crawler = new Crawler();
        try {
            crawler.readProperties();
            String root = crawler.props.getProperty("crawler.root");
            crawler.createDB();

            // crawler.crawl(root);
            
            
            doc = crawler.createDoc(root);
            description = doc.select("body").text();// "p"
            // // crawler.fetchURL(root, null);
            // // html = Jsoup.connect("http://www.cs.purdue.edu").get().html(); 
            // // doc = Jsoup.parse(html, "UTF-8");
            crawler.getLinks(doc);
            
        }
        catch( Exception e) {
            e.printStackTrace();
        } 
    }
}