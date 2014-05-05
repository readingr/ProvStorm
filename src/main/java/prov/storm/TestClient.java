package prov.storm;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class TestClient {

	
	
	
    public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException, InterruptedException, SAXException, ParserConfigurationException{
        //get the localhost IP address, if server is running on some other IP, you need to use that
        InetAddress host = InetAddress.getLocalHost();
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder builder = factory.newDocumentBuilder();	    
        
        

        while(true){
            socket = new Socket(host.getHostName(), 9876);
            ois = new ObjectInputStream(socket.getInputStream());
            String message = (String) ois.readObject();
            
            
            Document doc = builder.parse( new InputSource( new StringReader( message ) ) ); 

            NodeList nList = doc.getElementsByTagName("prov:wasDerivedFrom");
            
            //this reads the XML.
            for (int i = 0; i < nList.getLength(); i++) {
    			Node nNode = nList.item(i);
    			System.out.println(nNode.getNodeName());
    	        System.out.println(doc.getElementsByTagName("prov:entity").item(i).getAttributes().getNamedItem("prov:id").getTextContent());
    		}
            
//            System.out.println(doc.getElementsByTagName("prov:entity"));
//            System.out.println("Message: " + message);
            ois.close();
            Thread.sleep(100);
        }        
        
    }
    
    
}
