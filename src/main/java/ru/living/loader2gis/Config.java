package ru.living.loader2gis;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.util.ArrayList;

public class Config {
    protected static Config instance = null;

    public static Config getInstance() throws Exception
    {
        if (Config.instance == null)  {
            Config.instance = new Config();
        }
        return Config.instance;
    }

    public Settings db;

    public Settings redis;

    public ArrayList<City> cities;

    public ArrayList<Category> categories;


    public class Settings
    {
        public String server;
        public Integer port;
        public String db_name;
        public String login;
        public String password;
    }

    public class City
    {
        public String name;
        public Integer city_id;
        public Double latitude;
        public Double longitude;
        public Integer radius;
    }

    public class Category
    {
        public Integer living_category_id;
        public ArrayList<String> stop_words;
        public ArrayList<String> search_words;
        public ArrayList<Integer> rubrics;
    }

    public ArrayList<Integer> getAllCategories()
    {
        ArrayList<Integer> categories = new ArrayList<Integer>();
        for (Category category: this.categories) {
            categories.add(category.living_category_id);
        }
        return categories;
    }

    protected Config() throws Exception
    {

        this.cities = new ArrayList<City>();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(this.getClass().getClassLoader().getResourceAsStream("config.xml"));

        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xPath = xPathFactory.newXPath();

        XPathExpression expr = xPath.compile("/config/cities/city");
        Object result = expr.evaluate(doc, XPathConstants.NODESET);
        NodeList nodeList = (NodeList) result;

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            City city = new City();
            city.name = node.getAttributes().getNamedItem("name").getNodeValue();
            city.city_id = Integer.parseInt(node.getAttributes().getNamedItem("city_id").getNodeValue());
            city.radius = Integer.parseInt(node.getAttributes().getNamedItem("radius").getNodeValue());
            city.latitude = Double.parseDouble(node.getAttributes().getNamedItem("latitude").getNodeValue());
            city.longitude = Double.parseDouble(node.getAttributes().getNamedItem("longitude").getNodeValue());
            this.cities.add(city);
        }


        this.categories = new ArrayList<Category>();

        expr = xPath.compile("/config/categories/category");
        result = expr.evaluate(doc, XPathConstants.NODESET);
        nodeList = (NodeList) result;

        NodeList nodeList1;
        Node node1;

        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            Category category = new Category();
            category.living_category_id = Integer.parseInt(node.getAttributes().getNamedItem("living_category_id").getNodeValue());

            category.stop_words = new ArrayList<String>();
            expr = xPath.compile("stop_words/word/text()");
            result = expr.evaluate(node, XPathConstants.NODESET);
            nodeList1 = (NodeList) result;

            for (int j = 0; j < nodeList1.getLength(); j++) {
                node1 = nodeList1.item(j);
                category.stop_words.add(node1.getNodeValue());
            }

            category.search_words = new ArrayList<String>();
            expr = xPath.compile("search_words/word/text()");
            result = expr.evaluate(node, XPathConstants.NODESET);
            nodeList1 = (NodeList) result;

            for (int j = 0; j < nodeList1.getLength(); j++) {
                node1 = nodeList1.item(j);
                category.search_words.add(node1.getNodeValue());
            }




            category.rubrics = new ArrayList<Integer>();
            expr = xPath.compile("rubrics/rubric/text()");
            result = expr.evaluate(node, XPathConstants.NODESET);
            nodeList1 = (NodeList) result;

            for (int j = 0; j < nodeList1.getLength(); j++) {
                node1 = nodeList1.item(j);
                category.rubrics.add(Integer.parseInt(node1.getNodeValue()));
            }

            this.categories.add(category);
        }

        this.db = new Settings();
        this.parseSettings(this.db, doc, "/config/settings/db/*");

        this.redis = new Settings();
        this.parseSettings(this.redis, doc, "/config/settings/redis/*");
    }

    protected void parseSettings(Settings target, Document doc, String query) throws Exception
    {
        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xPath = xPathFactory.newXPath();

        XPathExpression expr = xPath.compile(query);
        Object result = expr.evaluate(doc, XPathConstants.NODESET);
        NodeList nodeList = (NodeList) result;

        for (int i = 0; i < nodeList.getLength(); i++) {
            if (nodeList.item(i).getNodeName().equalsIgnoreCase("server")) {
                target.server = nodeList.item(i).getFirstChild().getNodeValue();
            }

            if (nodeList.item(i).getNodeName().equalsIgnoreCase("port")) {
                target.port = Integer.parseInt(nodeList.item(i).getFirstChild().getNodeValue());
            }

            if (nodeList.item(i).getNodeName().equalsIgnoreCase("db_name")) {
                target.db_name = nodeList.item(i).getFirstChild().getNodeValue();
            }

            if (nodeList.item(i).getNodeName().equalsIgnoreCase("login")) {
                target.login = nodeList.item(i).getFirstChild().getNodeValue();
            }

            if (nodeList.item(i).getNodeName().equalsIgnoreCase("password")) {
                target.password = nodeList.item(i).getFirstChild().getNodeValue();
            }
        }
    }
}
