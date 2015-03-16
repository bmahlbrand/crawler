import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.jsoup.UnsupportedMimeTypeException;
import java.lang.*;

public class newCrawler {
    Connection connection;
    String tehContainer = null;
    int urlID;
    int occurence = 1;
    int queueCount = 0;
    int LIMIT = 2000;
    int QUEUE_LIMIT = 2500;
    boolean canTrip = false;
    String url = null;
    
    public Properties props;
    ArrayList<String> queue = new ArrayList<String>();
    ArrayList<String> linkzGotten = new ArrayList<String>();
    ArrayList<String> addedToDB = new ArrayList<String>();

    newCrawler() {
        urlID = 0;
    }
    
    public void readProperties() throws IOException {
        props = new Properties();
        FileInputStream in = new FileInputStream("database.properties");
        props.load(in);
        in.close();
    }

    public void setProperties() throws IOException {
        props = new Properties();
        FileInputStream in = new FileInputStream("database.properties");
        props.load(in);
        props.put("last_scanned", urlID);
        props.put("last_scanned_url", url);
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
        // stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(255), description VARCHAR(3000), UNIQUE(url))");
        // stat.executeUpdate("CREATE TABLE WORDS (urlid INT, word VARCHAR(40), occurence INT, CONSTRAINT uc_urlID UNIQUE(urlid, word))");
       // stat.executeUpdate("ALTER TABLE WORDS ADD CONSTRAINT UQ_urlid_word UNIQUE(urlid, word)");
    }
    
    public boolean urlInDB(String urlFound) throws SQLException, IOException {

        if (addedToDB.contains(urlFound))
            return true;
        // System.out.println("ran");
        return false;

        // Statement stat = connection.createStatement();
        

        // ResultSet result = stat.executeQuery( "SELECT * FROM urls WHERE url LIKE '"+urlFound+"'");

        // if (result.next()) {
        //     // System.out.println("URL "+urlFound+" already in DB");
        //     return true;
        // }
        // // System.out.println("URL "+urlFound+" not yet in DB");
        // return false;
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
        // if (!url.endsWith("/"))
        //     url = url + "/";
        try {

            String query = "INSERT IGNORE INTO urls (urlid, url, description) VALUES (?,?,?)";
            PreparedStatement stat = connection.prepareStatement(query);
            
            //System.out.println("Executing "+query);
            stat.setInt(1, urlID);
            stat.setString(2, url);
            stat.setString(3, description);
            addedToDB.add(url);
            // stat.setInt(4, occurence);
            
            int check = stat.executeUpdate();
            if (check != 0) {
                canTrip = true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NullPointerException n) {
            n.printStackTrace();
        }
        // }
    }

    public void insertWORDInDB(String word) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        String query = "INSERT INTO words VALUES ('"+urlID+"','"+word+"', '"+occurence+"') ON DUPLICATE KEY UPDATE occurence = occurence+1";//,'"+description+"')";
        stat.executeUpdate(query);
    }
    
    void addWordsToDB(String url, int urlID) throws SQLException, IOException {
        
        String description = createDoc(url).select("body").text();

        Pattern p = Pattern.compile("[^a-zA-Z0-9]");

        String[] arr = description.split(" ");
        int lawl = 0;
        for (String word : arr) {
            try {
                // if (lawl > 300)
                System.out.println(word);
                //     break;
                // if(!urlInDB(url)) {
                boolean hasSpecialChar = p.matcher(word).find();
                if (!hasSpecialChar) {
                        // if (!wordInDB(word)) {
                            // System.out.println(word);
                    insertWORDInDB(word.toLowerCase());
                        // } else {
                            //increment word counter
                        // }
                }                    
                // }
                    lawl++;
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    String getDescription(Document doc) {
        String description = null;

        // if (doc.select("div.header") != null) {
        //     for(Element element : doc.select("div.header"))
        //         element.remove();
        // }
        
        // if (doc.select("div.footer") != null) {
        //     for(Element element : doc.select("div.footer")) 
        //         element.remove();
        // }
        
        // if (doc.select("div.legal") != null) {
        //     for(Element element : doc.select("div.legal")) 
        //         element.remove();
        // }
        
        if (doc.select("div.main").text() != null && doc.select("div.main").text().length() > 50) {
            description = doc.select("div.main").text();
            // System.out.println("amg1");
        } else if (doc.select("p").text().length() > 5){
            description = doc.select("p").text();
            // System.out.println("amg2");
        } else if (doc.text() != null){
            description = doc.select("body").text();
            // System.out.println("amg3");
        }
        
        if (description != null && description.length() > 100) {
           description = description.substring(0, 99);
        } else if (description.length() == 0) {
            description = "no text found.";
        }

        return description.trim();
    }
    
    boolean addable(Element link)  {
        String check = link.absUrl("href");

        try {  
            if(link.absUrl("href") != null 
                && link.absUrl("href").contains(tehContainer)
                && !link.ownText().isEmpty()
                && (!link.absUrl("href").toLowerCase().endsWith(".pdf") && !link.absUrl("href").toLowerCase().endsWith(".pdf/"))
                && (!link.absUrl("href").endsWith(".png") && !link.absUrl("href").endsWith(".png/"))
                && (!link.absUrl("href").endsWith(".jpg") && !link.absUrl("href").endsWith(".jpg/"))
                && (!link.absUrl("href").endsWith(".gif") && !link.absUrl("href").endsWith(".gif/"))
                && (!link.absUrl("href").endsWith(".h") && !link.absUrl("href").endsWith(".h/"))
                && (!link.absUrl("href").endsWith(".c") && !link.absUrl("href").endsWith(".c/"))
                && (!link.absUrl("href").endsWith(".m") && !link.absUrl("href").endsWith(".m/"))
                && (!link.absUrl("href").endsWith(".java") && !link.absUrl("href").endsWith(".java/"))
                && (!link.absUrl("href").endsWith(".pps") && !link.absUrl("href").endsWith(".pps/"))
                && (!link.absUrl("href").endsWith(".mpg") && !link.absUrl("href").endsWith(".mpg/"))
                && (!link.absUrl("href").endsWith(".wmv") && !link.absUrl("href").endsWith(".wmv/"))
                && (!link.absUrl("href").endsWith(".mp3") && !link.absUrl("href").endsWith(".mp3/"))
                && !link.absUrl("href").contains("ftp:")
                && !link.absUrl("href").contains("#") 
                && !link.absUrl("href").contains("?") 
                && !link.absUrl("href").contains("=") 
                && !link.absUrl("href").endsWith(".php")
                && !link.absUrl("href").contains(".asp")
                && !link.absUrl("href").contains("mailto:")
                && !link.absUrl("href").contains(".shtml")
                // && link.absUrl("href").endsWith(".html")

                && !urlInDB(link.absUrl("href"))
                // && !linkzGotten.contains(link.absUrl("href"))
                // && (createDoc(link.absUrl("href")).select("body").text() != null)
                && !queue.contains(link.absUrl("href"))) {

                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    void addToQueue(Elements links) {

        try {
            for (Element link : links) {
                if(addable(link)) {
                    queue.add(link.absUrl("href"));
                    queueCount++;
                    // System.out.println("adding");
                    // System.out.println(link.absUrl("href"));
                    // System.out.println(queueCount);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void getLinks(Document doc) {
        String next = null;
        int marker = 0, count = 0;

        try {
            Elements links = doc.select("a");
            addToQueue(links);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    Document createDoc(String url) {
        try {
            if (!url.contains("slider")) {
                // System.out.println(url);
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
        } catch (Exception e) {
            return null;
        }
    }

    void crawl(String root) throws SQLException, IOException {
       
        Document doc = null;
        String description = null;
        Elements links = null;
        //String description;
        //   while (NextURLIDScanned < NextURLID) {
        //     urlIndex = NextURLIDScanned;
        //     Fetch the url1 entry in urlIndex
        url = root;
        try {
            openConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        while (urlID < 7000) {
            // System.out.printf("%d \n", queueCount);
            try {

                if (createDoc(url) != null && createDoc(url).select("body").text() != null && getDescription(createDoc(url)).length() != 0 ) doc = createDoc(url);
                
                if (queueCount < 8000 && !linkzGotten.contains(url)){
                    getLinks(doc);
                    linkzGotten.add(url);
                }

                description = getDescription(doc);
                
                if (description != null && !description.equals("no text found.")) {
                    insertURLInDB(url, description);
                    System.out.println(url);
                    
                    if (canTrip) {
                        addWordsToDB(url, urlID);
                        urlID++;
                    }
                    canTrip = false;
                    // 
                }

                if (queue.isEmpty())
                    break;
                url = queue.get(0);
                queue.remove(0);
                
            } catch(Exception e) {
                
                while (createDoc(url) == null) {
                    if (queue.isEmpty())
                        break;
                    url = queue.get(0);
                    queue.remove(0);
                }

                System.out.printf("%s failed\n", url);
                e.printStackTrace();
            }
           // links = doc.select("a[href]");
        } 
    }

    void addAlldahWordz() throws SQLException {
        try {
            openConnection();
            Statement stat = connection.createStatement();
            ResultSet result = stat.executeQuery("SELECT * FROM urls");
            
            while (result.next()) {
                System.out.println("amg");
                int url_word_id = result.getInt("urlID");
                String url = result.getString("url");
                addWordsToDB(url, url_word_id);
            }
        } catch (Exception e) {

        }
    }

    private static void WriteQueue(ArrayList<String> queue) {  
        try {  
            FileOutputStream fos = new FileOutputStream ("keep.dat");  
            ObjectOutputStream oos = new ObjectOutputStream(fos);  
            
            oos.writeObject(queue);  

            fos.close();  

        } catch (Exception e) {  
            System.out.println(e);     
        }  
    }

     private static void WriteLinkzGotten(ArrayList<String> linkzGotten) {  
        try {  
            FileOutputStream fos2 = new FileOutputStream ("keep2.dat");  
            ObjectOutputStream oos2 = new ObjectOutputStream(fos2);  
             
            oos2.writeObject(linkzGotten); 
            fos2.close();

        } catch (Exception e) {  
            System.out.println(e);     
        }  
    }

     private static void WriteAddedToDB(ArrayList<String> addedToDB) {  
        try {  
            FileOutputStream fos3 = new FileOutputStream ("keep3.dat");  
            ObjectOutputStream oos3 = new ObjectOutputStream(fos3);  
            
            oos3.writeObject(addedToDB);
            fos3.close();

        } catch (Exception e) {  
            System.out.println(e);     
        }  
    }

    private static ArrayList<String> ReadQueue(){  
        ArrayList<String> queue = new ArrayList<String>();  
        try {  
            FileInputStream fis = new  FileInputStream("keep.dat");  
            ObjectInputStream ois = new ObjectInputStream(fis);  
            Object obj = ois.readObject();  
            queue = (ArrayList<String>) obj;  
        } catch (Exception e) {  
            System.out.println(e);  
        }   

        return queue;  
    }
    
    private static ArrayList<String> ReadLinkzGotten(){  
        ArrayList<String> linkzGotten = new ArrayList<String>();  
        try {  
            FileInputStream fis = new  FileInputStream("keep2.dat");  
            ObjectInputStream ois = new ObjectInputStream(fis);  
            Object obj = ois.readObject();  
            linkzGotten = (ArrayList<String>) obj;  
        } catch (Exception e) {  
            System.out.println(e);  
        }   
        
        return linkzGotten;  
    }

    private static ArrayList<String> ReadAdded(){  
        ArrayList<String> addedToDB = new ArrayList<String>();  
        try {  
            FileInputStream fis = new  FileInputStream("keep3.dat");  
            ObjectInputStream ois = new ObjectInputStream(fis);  
            Object obj = ois.readObject();  
            addedToDB = (ArrayList<String>) obj;  
        } catch (Exception e) {  
            System.out.println(e);  
        }   
        
        return addedToDB;  
    }
    
    // public class Shutdown() implements Runnable {
    //     Runtime.getRuntime().addShutdownHook(new Thread() {
    //         public void run() { 
    //             setProperties();
    //             System.out.println("successful shutdown.");
    //             System.out.exit(0);
    //         }
    //     });
    // }

    public static void main(String[] args) {   
        String html = null;
        Document doc = null;
        String description = null;

        newCrawler crawler = new newCrawler();
        
        // Shutdown shutdown = new Shutdown();
        // Write(crawler.queue);
            // crawler.queue = ReadQueue();
            // crawler.linkzGotten = ReadLinkzGotten();
            // crawler.addedToDB = ReadAdded();

       

        try {

    
            // crawler.urlID = 5150;
            crawler.readProperties();
            
            String root = crawler.props.getProperty("crawler.root");
            String root2 = crawler.props.getProperty("crawler.root2");
            String root3 = crawler.props.getProperty("crawler.root3");
            String root4 = crawler.props.getProperty("crawler.root4");
            
            // crawler.createDB();
            crawler.openConnection();
            crawler.tehContainer = "cs.purdue.edu";

            crawler.crawl(root3);
            crawler.LIMIT = 3000;
            crawler.QUEUE_LIMIT = 4000;
            crawler.tehContainer = "cs.purdue.edu";
            crawler.crawl(root2);
            crawler.LIMIT = 6000;
            crawler.QUEUE_LIMIT = 8000;

            crawler.tehContainer = "cs.purdue.edu";
            crawler.QUEUE_LIMIT += 1000;
            crawler.LIMIT += 2000;
            crawler.crawl(root4);

            crawler.tehContainer = "purdue.edu";
            crawler.LIMIT += 2000;
            crawler.crawl(root);
            
            // System.out.println(crawler.queue.toString());
            //crawler.addAlldahWordz();
            // doc = crawler.createDoc(root);
            // description = doc.select("body").text();// "p"
            // // crawler.fetchURL(root, null);
            // // html = Jsoup.connect("http://www.cs.purdue.edu").get().html(); 
            // // doc = Jsoup.parse(html, "UTF-8");
            // crawler.getLinks(doc);
            
        } catch(Exception e) {

            e.printStackTrace();
        } 
    }
}