package prov.storm;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class testing {

	
	public static void main(String[] args) throws SAXException, IOException, ParserConfigurationException, XPathExpressionException {
		String t = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
				"<prov:document xmlns:prov=\"http://www.w3.org/ns/prov#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://example.org\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\n" + 
				"    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e2\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e1\"/>\n" + 
				"    </prov:wasDerivedFrom>\n" + 
				"    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e3\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e2\"/>\n" + 
				"    </prov:wasDerivedFrom>\n" + 
				"</prov:document>\n" + 
				"";
		final String xmlStr = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"+
                "<Emp id=\"1\"><name id=\"whore\">Pankaj</name><age>25</age>\n"+
                "<role>Developer</role><gen>Male</gen></Emp>";
		
		
		
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    DocumentBuilder builder = factory.newDocumentBuilder();	    
        Document doc = builder.parse( new InputSource( new StringReader( t ) ) ); 

        NodeList nList = doc.getElementsByTagName("prov:wasDerivedFrom");
        
    
        
        
        //this reads the XML.
        for (int i = 0; i < nList.getLength(); i++) {
			Node nNode = nList.item(i);
			
			//cast to element so we can traverse.
			Element docElement = (Element)nNode;
		
			System.out.println(docElement.getElementsByTagName("prov:generatedEntity").item(0).getAttributes().getNamedItem("prov:ref").getTextContent());			
			System.out.println(nNode.getNodeName());
			System.out.println(docElement.getElementsByTagName("prov:usedEntity").item(0).getAttributes().getNamedItem("prov:ref").getTextContent());			
			System.out.println("\n");
        }
        

	}
	
	
}
