import java.util.ArrayList;
import org.apache.hadoop.hbase.client.Put;

/**
 * BuildInputData class defines row-key for each record and breaks down every json object into key-value pairs.
 */
public class HbaseBuildInputData 
{	
	/**
	 * Defines row-key for each json object.
	 * @param	line	is an array-list of string type which contains all the key-value pairs of the object
	 * @param	type	defines the type of json object, whether it is user, or review, or business
	 * @return		returns row-key
	 */
	public static String getRowKeyForPutData(ArrayList<String> line, String type)
	{
		String tempValues[];
			
		if (type == "User Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("user_id"))
				{	
					tempValues = line.get(i).split(":");	
					System.out.println("row-key for user object is: " + tempValues[1].trim());								
					return tempValues[1].trim();
				}							
			}
		}
		
		else if (type == "Review Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("review_id"))
				{	
					tempValues = line.get(i).split(":");	
					System.out.println("row-key for review object is: " + tempValues[1].trim());								
					return tempValues[1].trim();
				}							
			}
		}
			
		else if (type == "Business Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("business_id"))
				{	
					tempValues = line.get(i).split(":");	
					System.out.println("row-key for business object is: " + tempValues[1].trim());								
					return tempValues[1].trim();
				}							
			}
		}
	
		return "no row-key";
	}
	
	/**
	 * Creates put statement to load the json objects in its respective tables.
	 * @param	line	is an array-list of string type which contains all the key-value pairs of the object
	 * @param	type	defines the type of json object, whether it is user, or review, or business
	 * @throws	UserExceptions	user exception is thrown if there is no row-key, or the size is more for any json object
	 */
	public static void buildPutData(ArrayList<String> line, String type) throws UserExceptions 
	{	
		String tempValues[];
		
		String rowKey = getRowKeyForPutData(line, type); 
		
		if (rowKey.equals("no row-key"))
		{
			throw new UserExceptions("no row-key exception");
		}
		
		Put putLine = new Put(rowKey.getBytes());
					
		if (type == "User Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("votes"))
				{
					tempValues = line.get(i).split(",");					
					for (int j = 0; j < tempValues.length; j++)
					{
						String temp[] = tempValues[j].split(":");
						if (temp.length == 3)
						{
							putLine.addColumn("User Object".getBytes(), temp[1].trim().getBytes(), temp[2].getBytes());	
						}
						else
						{
							putLine.addColumn("User Object".getBytes(), temp[0].trim().getBytes(), temp[1].getBytes());	
						}
					}
				}
				else if (line.get(i).contains("url"))
				{
					tempValues = line.get(i).split(":", 2);
					putLine.addColumn("User Object".getBytes(), tempValues[0].trim().getBytes(), tempValues[1].getBytes());		
				}
				else
				{
					tempValues = line.get(i).trim().split(":");
					putLine.addColumn("User Object".getBytes(), tempValues[0].trim().getBytes(), tempValues[1].getBytes());				
				}
			}
			
			if (line.size() > 7)
			{
				throw new UserExceptions("increased user object size " + line.size());
			}
			System.out.println("\n put object: " + putLine);		
		}
		
		else if (type == "Review Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("\"votes\": "))
				{
					tempValues = line.get(i).trim().split(",");					
					for (int j = 0; j < tempValues.length; j++)
					{
						String temp[] = tempValues[j].split(":");
						if (temp.length == 3)
						{
							putLine.addColumn("Review Object".getBytes(), temp[1].trim().getBytes(), temp[2].getBytes());	
						}
						else
						{
							putLine.addColumn("Review Object".getBytes(), temp[0].trim().getBytes(), temp[1].getBytes());	
						}
					}
				}
				else
				{
					tempValues = line.get(i).split(":");
					putLine.addColumn("Review Object".getBytes(), tempValues[0].trim().getBytes(), tempValues[1].getBytes());				
				}
			}
			
			if (line.size() > 10)
			{
				throw new UserExceptions("increased review object size " + line.size());
			}
			System.out.println("\n put object for review object: " + putLine);		
		}
		
		else if (type == "Business Object")
		{
			for (int i = 0; i < line.size(); i++)
			{
				if (line.get(i).contains("url") || line.get(i).contains("photo_url"))
				{
					tempValues = line.get(i).split(":", 2);
					putLine.addColumn("Business Object".getBytes(), tempValues[0].trim().getBytes(), tempValues[1].getBytes());		
				}
				
				else
				{
					tempValues = line.get(i).split(":");
					putLine.addColumn("Business Object".getBytes(), tempValues[0].trim().getBytes(), tempValues[1].getBytes());
				}			
			}

			if (line.size() > 16)
			{
				throw new UserExceptions("increased business object size " + line.size());
			}
			System.out.println("\n put object for business object: " + putLine);	
		}
		
		HbaseDBConnect.loadData(putLine);
	}
}
