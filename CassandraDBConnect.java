import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


/**
 * CassandraDBConnect class connects to the Cassandra data store, create keyspace and tables, read data file, and store json-objects.
 */
public class CassandraDBConnect 
{
	private static Cluster cluster;
    static Session session;
	static BufferedReader reader = null;
	static String line = null;
    static int count = 0;
	
    /**
     * Adds the contact point to the Cluster object using which we connect to the Cassandra.
     * @param	node	contact point to try connecting for cluster discovery
     * @return			Cluster object which now has the information of the contact point
     */
	public static Cluster connect(String node)
	{
		return Cluster.builder().addContactPoint(node).build();
	}
	
	/**
	 * Connects to the data store.
	 */
	public static void connectDB()
	{
		cluster = connect("localhost");
		session = cluster.connect();
	}
	
	/**
	 * Closes the connection with the data store.
	 */
	public static void closeDB()
	{
		session.close();
		cluster.close();	
	}
	
	/**
	 * Creates a keyspace in the data store.
	 * @param	keyspaceName	name of the keyspace which is to be created
	 */
	public static void createKeyspace(String keyspaceName)
	{
		session.execute("CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};");
	}
	
	/**
	 * To use a particular keyspace.
	 * @param	keyspaceName	name of the keyspace which is to be used
	 */
	public static void useKeyspace(String keyspaceName)
	{
		session.execute("USE " + keyspaceName);
	}
	
	/**
	 * Create tables in the keyspace.
	 */
	public static void createTables()
	{
		session.execute("CREATE TABLE businessobjects (type text, name text, business_id text PRIMARY KEY, neighborhoods list<text>, open boolean, "
		+ "full_address text, latitude float, url text, longitude float, stars float, review_count int, photo_url text, city text, state text, "
	    + "categories list<text>, schools list<text>);");

        session.execute("CREATE TABLE reviewobjects (type text, business_id text, date date, funny int, useful int, cool int, user_id text, "
		+ "review_id text PRIMARY KEY, stars int, text text);");

        session.execute("CREATE TABLE userobjects (type text, user_id text PRIMARY KEY, name text, url text, review_count int, average_stars float, "
	    + "funny int, useful int, cool int);");
	}
	
	/**
	 * Reads json data from .json file and stores the objects in its respective tables.
	 */
	public static void readFile()
	{
		try   
		{
		    File file = new File("/home/pinkpanther/Documents/6_Credits_Project/yelp_academic_dataset.json");
		    
		    reader = new BufferedReader(new FileReader(file));
		    
		    while ((line = reader.readLine()) != null) 
		    {
		    	System.out.println("\n " + (++count) + line);
		    	
		    	line = line.substring(1, line.length() - 1);
		    	line = line.replaceAll("'", "''"); 		    	
		    	
		    	if (line.contains("\"votes\""))
	        	{
	        		line = line.replaceFirst("\"votes\": ", " ");	
	        		line = line.substring(2, line.length());
	        		line = line.replaceFirst("}", " ");
	        	}
		        
		        if (line.contains("\"type\": \"user\"")) 
		        {
		        	session.execute("INSERT INTO userobjects JSON '{  " + line + "}';");     	
		        }
		        
		        else if (line.contains("\"type\": \"review\"")) 
		        {		        		
			        session.execute("INSERT INTO reviewobjects JSON '{  " + line + "}';");
		        	
		        	System.out.println("\n " + line);		        	
		       	}
		        
		        else if (line.contains("\"type\": \"business\"")) 
		        {
		        	System.out.println("\n " + line);
		        	
		        	session.execute("INSERT INTO businessobjects JSON '{  " + line + "}';");
		        }	        		        
		    }
		} 		
		catch (IOException e) 
		{
		    e.printStackTrace();
		} 
		finally 
		{
		    try 
		    {
		        reader.close();
		    } 
		    catch (IOException e) 
		    {
		        e.printStackTrace();
		    }
		}
	}
	
	public static void main(String[] args) throws Exception {
		connectDB();
		//createKeyspace("yelpkeys");
		useKeyspace("yelpkeys");
        //createTables();
		//readFile();
		CassandraQueries.viewContentsBusiness();
		CassandraQueries.totalReviews();
		CassandraQueries.countReviewsState();
		CassandraQueries.businessHighReviewCount();
		CassandraQueries.numberRestaurants();
		CassandraQueries.listRestaurants();
		//CassandraQueries.firstCategoriesReviewCount(); ---doesn't work
		//CassandraQueries.businessesCoolReviews(); ---doesn't work
		CassandraQueries.viewUserObjects();
		closeDB();		
	}
}
