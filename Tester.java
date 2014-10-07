import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;
import org.jsoup.UnsupportedMimeTypeException;

import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

public class Tester {

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
        }
    }

    void descriptionFilter(Node node) {
    	int i = 0;
        while (i < node.childNodes().size()) {
            Node child = node.childNode(i);
            if (child.nodeName().equals("div.header") || child.nodeName().equals("div.footer") || child.nodeName().equals("div.legal"))
                child.remove();
            else {
                descriptionFilter(child);
                i++;
            }
        }
    }

    String getDescription(Document doc) {
        String description = null;

        for(Element element : doc.select("div.header")) {
            element.remove();
        }

        for(Element element : doc.select("div.footer")) {
            element.remove();
        }

        for(Element element : doc.select("div.legal")) {
            element.remove();
        }

        if (doc.select("div.main").text() != null) {
        	description = doc.select("div.main").text();
        	System.out.println("amg1");
        } else if (doc.select("p").text().length() > 50){
        	description = doc.select("p").text();
        	System.out.println("amg2");
        } else {
        	description = doc.select("body").text();
        	System.out.println("amg3");
        }
        

        if (description != null && description.length() > 3000) {
           description = description.substring(0, 2999);
        } else if (description == null) {
            description = "no text found.";
        }

        return description;
    }
	public static void main(String[] args) { 
		Tester tester = new Tester();
		System.out.println(tester.getDescription(tester.createDoc("http://www.cs.purdue.edu/")));

	}
}
