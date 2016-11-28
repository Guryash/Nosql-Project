import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

/*import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;*/

/**
 * DBConnect class connects to the HBase data store, create tables and column families, read data file, and store json-objects.
 */
public class HbaseDBConnect 
{
	static Configuration conf;
	static Connection connection;	
    static Admin admin;
    static Table table;
    
    /**
     * Connects to the data store.
     * @throws	IOException   for objects, connection and admin
     */
	public static void connect() throws IOException
	{
		conf = HBaseConfiguration.create();
		/*conf.set("hbase.zookeeper.property.dataDir","/home/guryash/zookeeper"); --- not required*/
		conf.set("hbase.zookeeper.property.clientPort","2181");
		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
	}
	
	/**
	 * Creates table and column families for that table.
	 * @throws	IOException   for object, admin		
	 */
	public static void createTableColumnFamily() throws IOException
	{
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("yelpkeys"));
        tableDescriptor.addFamily(new HColumnDescriptor("User Object"));
        tableDescriptor.addFamily(new HColumnDescriptor("Review Object"));
	    tableDescriptor.addFamily(new HColumnDescriptor("Business Object"));
	    admin.createTable(tableDescriptor);
	}
    
	public static void main(String[] args) throws Exception 
	{ 
		connect();
		//createTableColumnFamily();	    
		//HbaseReadFile2.readFile("/home/pinkpanther/Documents/6_Credits_Project/yelp_academic_dataset.json");
		getTable("yelpkeys");
		//HbaseQueries.listTables(admin);
		//HbaseQueries.listTablesMatchingExpression(admin);
		//HbaseQueries.checkTableDisable(admin);
		//HbaseQueries.checkTableEnable(admin);
		//HbaseQueries.disableTableMatchingExpression(admin);
		//HbaseQueries.enableTableMatchingExpression(admin);
		//HbaseQueries.scanTable(table);
		HbaseQueries.getValue(table, "zzyU8aS_GoDUFiVEFFX20g", "Review Object", "review_id");
		//HbaseQueries.deleteRow(table, "zzyU8aS_GoDUFiVEFFX20g");
		HbaseQueries.updateValue(table, "zzyU8aS_GoDUFiVEFFX20g", "Review Object", "review_id", "zzyU8aS_GoDUFiVEFFX20g");
	}
	
	/**
	 * Table object gets connection to the mentioned table in the data store.
	 * @param	tableName	is the name of the table 
	 * @throws	IOException	is thrown when connection is refused to the mentioned table
	 */
	public static void getTable(String tableName) throws IOException
	{
		table = connection.getTable(TableName.valueOf(tableName));	
	}
	
	/**
	 * Loads the data into the data store.
	 * @param	putData		is the data, one key-value pair at a time, that is loaded into the data store	
	 */
	public static void loadData(Put putData)
	{
		try
		{
			getTable("yelpkeys");
			table.put(putData);			
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}	
}
