import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Stopwatch;


/**
 * Queries class  contains different queries to fetch data from the data store.
 */
public class HbaseQueries {
	
	static Stopwatch timer = new Stopwatch();
	
	/**
	 * Query 1: List all the tables in the data store.
	 * @param	admin	is the object which is connected to the data store	
	 * @throws	IOException	for object, admin
	 */
	public static void listTables(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 1----------");
		
		timer.start();
		HTableDescriptor listTables[] = admin.listTables();
	    timer.stop();
	    System.out.println("\nTime elapsed to execute a query, listTables: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();

		System.out.println("Following are the tablespresent in the database:");
		for (int i = 0; i < listTables.length; i++)
		{
			System.out.println(listTables[i].getTableName());
		}
	}
	
	/**
	 * Query 2: List all the tables which matches the given expression, "yelp.*".
	 * @param	admin	is the object which is connected to the data store
	 * @throws	IOException	for object, admin
	 */
	public static void listTablesMatchingExpression(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 2----------");
		
		timer.start();
		HTableDescriptor listTablesMatchingExpression[] = admin.listTables("yelp.*");
		timer.stop();
	    System.out.println("\nTime elapsed to execute a query, listTablesMatchingExpression: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();

		System.out.println("Following are the tables matched with expression yelp.*:");
		for (int i = 0; i < listTablesMatchingExpression.length; i++)
		{
			System.out.println(listTablesMatchingExpression[i].getTableName());
		}
	}

	/**
	 * Query 3: To check whether the table, yelp, is disabled and if not, then it is disabled.
	 * @param	admin	is the object which is connected to the data store
	 * @throws	IOException	for object, admin
	 */
	public static void checkTableDisable(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 3----------");
		
		timer.start();
		boolean bDisable = admin.isTableDisabled(TableName.valueOf("yelpkeys"));
		timer.stop();
		System.out.println("\nyelpkeys table is disabled or not: " + bDisable);
		if (!bDisable)
		{
			timer.start();
			admin.disableTable(TableName.valueOf("yelpkeys"));
			bDisable = admin.isTableDisabled(TableName.valueOf("yelpkeys"));
			timer.stop();
		    System.out.println("yelpkeys table is disabled: " + bDisable);
		}
		System.out.println("Time elapsed to execute a query, checkTableDisable: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();
	}
	
	/**
	 * Query 4: To check whether the table, yelp, is enabled and if not, then it is enabled.
	 * @param	admin	is the object which is connected to the data store
	 * @throws	IOException	for object, admin
	 */
	public static void checkTableEnable(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 4----------");
		
		timer.start();
		boolean bEnable = admin.isTableEnabled(TableName.valueOf("yelpkeys"));
		timer.stop();
		System.out.println("\nyelpkeys table is enabled or not: " + bEnable);
		if (!bEnable)
		{
			timer.start();
			admin.enableTable(TableName.valueOf("yelpkeys"));
			bEnable = admin.isTableEnabled(TableName.valueOf("yelpkeys"));
			timer.stop();
		    System.out.println("yelpkeys table is enabled: " + bEnable);
		}
		System.out.println("Time elapsed to execute a query, checkTableEnable: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();
	}
	
	/**
	 * Query 5: To disable table(s) which match the given expression, "yel.*".
	 * @param	admin	is the object which is connected to the data store
	 * @throws	IOException	for object, admin
	 */
	public static void disableTableMatchingExpression(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 5----------");
		
		HTableDescriptor listTablesMatchingExpression[] = admin.listTables("yel.*");
		System.out.println("\nFollowing are the tables matched with expression \"yel.*\":");
		for (int i = 0; i < listTablesMatchingExpression.length; i++)
		{
			System.out.println(listTablesMatchingExpression[i].getTableName());
		}
		
		timer.start();
		admin.disableTables("yel.*");
		timer.stop();
		System.out.println("Time elapsed to execute a query, disableTableMatchingExpression: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();
		
	    System.out.println("Following tables are disabled:"); 
		for (int i = 0; i < listTablesMatchingExpression.length; i++)
		{
			System.out.println(listTablesMatchingExpression[i].getTableName() + ": " + admin.isTableDisabled(listTablesMatchingExpression[i].getTableName()));
		}
	}

	/**
	 * Query 6: To enable table(s) which match the given expression, "yel.*".
	 * @param	admin	is the object which is connected to the data store
	 * @throws	IOException	for object, admin
	 */
	public static void enableTableMatchingExpression(Admin admin) throws IOException
	{
		System.out.println("\n\n----------Query 6----------");
		
		HTableDescriptor listTablesMatchingExpression[] = admin.listTables("yel.*");
		System.out.println("\nFollowing are the tables matched with expression \"yel.*\":");
		for (int i = 0; i < listTablesMatchingExpression.length; i++)
		{
			System.out.println(listTablesMatchingExpression[i].getTableName());
		}
		
		timer.start();
		admin.enableTables("yel.*");
		timer.stop();
		System.out.println("Time elapsed to execute a query, disableTableMatchingExpression: " + timer.elapsedMillis() + " milliseconds.");
	    timer.reset();
	    
		System.out.println("Following tables are enabled:"); 
		for (int i = 0; i < listTablesMatchingExpression.length; i++)
		{
			System.out.println(listTablesMatchingExpression[i].getTableName() + ": " + admin.isTableEnabled(listTablesMatchingExpression[i].getTableName()));
		}
	}

	/**
	 * Query 7: To scan the whole table in the data store.
	 * @param	table	is the table to which the connection is made
	 * @throws	IOException	is thrown when the getScanner operation is not performed or in case of scanner.next 
	 */
	public static void scanTable(Table table) throws IOException
	{
		System.out.println("\n\n----------Query 7----------");
				
		Scan scan = new Scan();
		timer.start();
		ResultScanner scanner = table.getScanner(scan);
		timer.stop();
		 try 
		 {
			 int count = 1;
			 for (Result rr = scanner.next(); rr != null; rr = scanner.next()) 
			 {
				 System.out.println("Found row " + count + ": " + rr);
				 count++;
	         }
		 }
		 finally 
		 {
			 scanner.close();
			 System.out.println("Time elapsed to execute a query, scanTable: " + timer.elapsedMillis() + " milliseconds.");
			 timer.reset();
		 }
	}
	
	/**
	 * Query 8: To get column value of a column family for a given row-key.
	 * @param	table	is the table to which the connection is made
	 * @param	rowKey	points to a particular row
	 * @param	columnFamily	is the name of a column family
	 * @param	qualifier	is the column name belonging to the columnFamily
	 * @throws	IOException	is thrown if the get operation is not performed 
	 */
	public static void getValue(Table table, String rowKey, String columnFamily, String qualifier) throws IOException
	{
		System.out.println("\n\n----------Query 8----------");
		
		Get g = new Get(rowKey.getBytes());
		timer.start();
        Result result = table.get(g);
        timer.stop();
        System.out.println("Is the deleted row empty: " + result.isEmpty());
        System.out.println(result);
        System.out.println("Time elapsed to execute a query, getValue: " + timer.elapsedMillis() + " milliseconds.");
		timer.reset();
        System.out.println(Bytes.toString(result.getValue(columnFamily.getBytes(), qualifier.getBytes())));
	}
	
	/**
	 * Query 9: To delete a particular row.
	 * @param	table	is the table to which the connection is made
	 * @param	rowKey	points to a particular row
	 * @throws	IOException	is thrown when table is not able to perform either delete operation or get operations
	 */
	public static void deleteRow(Table table, String rowKey) throws IOException
	{
		System.out.println("\n\n----------Query 9----------");
		
		Delete del = new Delete(rowKey.getBytes());
		timer.start();
        table.delete(del);
        timer.stop();
        System.out.println("Time elapsed to execute a query, deleteRow: " + timer.elapsedMillis() + " milliseconds.");
  		timer.reset();
  		
        Get g = new Get(rowKey.getBytes());
        Result result = table.get(g);
        System.out.println("Is the deleted row empty: " + result.isEmpty());
	}
	
	/**
	 * Query 10: To update the value of a column.
	 * @param	table	is the table to which the connection is made
	 * @param	rowKey	points to a particular row
	 * @param	columnFamily	is the name of a column family
	 * @param	qualifier	is the column name belonging to the columnFamily
	 * @param	newValue	is the new value for the column
	 * @throws	IOException	is thrown when either put operation or get operation is not performed
	 * @throws	UserExceptions	i thrown when the mentioned column is not updated with the new value
	 */
	public static void updateValue(Table table, String rowKey, String columnFamily, String qualifier, String newValue) throws IOException, UserExceptions
	{
		System.out.println("\n\n----------Query 10----------");
		
		Put put = new Put(rowKey.getBytes());
		put.addColumn(columnFamily.getBytes(), qualifier.getBytes(), newValue.getBytes());
		timer.start();
        table.put(put);
        timer.stop();
        System.out.println("Time elapsed to execute a query, updateValue: " + timer.elapsedMillis() + " milliseconds.");
  		timer.reset();
		
  		 Get g = new Get(rowKey.getBytes());
         Result result = table.get(g);
         if (!Bytes.toString(result.getValue(columnFamily.getBytes(), qualifier.getBytes())).equals(newValue))
         {
        	 throw new UserExceptions("Value is not updated.");
         }
	}
}
