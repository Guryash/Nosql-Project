import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class HbaseReadFile2 
{
	/**
	 * Reads json data from .json file.
	 */
	public static void readFile(String filePath) throws UserExceptions 
	{
		BufferedReader reader = null;
		String line = null;
        int count = 0;
		try 
		{
		    File file = new File(filePath);
		    reader = new BufferedReader(new FileReader(file));
		    
		    while ((line = reader.readLine()) != null) 
		    {
		    	System.out.println("\n object " + (++count));
		    	
		    	line = line.substring(1, line.length() - 1);
		    	line = line.replace("\"", "");
		    	line = line.replace("{", "");
		    	
		    	System.out.println(line);
		        
		        if (line.contains("type: user")) 
		        {
		        	String user[] = line.split("}");
		        	int userLength = user.length;
		        	ArrayList<String> userData = new ArrayList<String>();
		        	
		        	if (userLength == 2)
		        	{
		        		userData.add(user[0]);

		        		String userTemp[] = user[1].replaceFirst(",", "").trim().split(",");
		        		boolean extraCommaIndicator = true;
	        			int loopCount = 0;
		        		while (extraCommaIndicator)
		        		{
		        			for (int i = 0; i < userTemp.length; i++)
		        			{
		        				while (!userTemp[i].contains(": "))
		        			    {
		        					loopCount++;
		        					userTemp[i - 1] = userTemp[i - 1].concat(userTemp[i]).replace(",", "");
			        			    for (int j = i; j < userTemp.length - 1; j++)
			        			    { 
			        			    	i++;
			        				    userTemp[j] = userTemp[i];			        				
			        			    }
		        			    }
		        			}
		        			
		        			if (userTemp.length - loopCount == 6)
		        			{
		        				extraCommaIndicator = false;
		        				for (int i = 0; i < userTemp.length - loopCount; i++)
				        		{
				        			userData.add(userTemp[i]);	        			        		
				        		}	
		        			}	
		        		}
		        	        			        			
		        		for (int i = 0; i < userData.size(); i++)
		        		{
		        			System.out.println("\n " + i + ": " + userData.get(i));
		        		}
		        		
		        		HbaseBuildInputData.buildPutData(userData, "User Object");
		        	}
		        	else 
		        	{
		        		throw new UserExceptions("Invalid User Object" + line);
		        	}
		        }
		        
		        else if (line.contains("type: review")) 
		        {
		        	ArrayList <String> reviewData = new ArrayList<String>();
		        	String review[] = line.split("}", 2);
		        			        	
		        	reviewData.add(review[0]);  
		        	
		       		String reviewData1[] = review[1].replaceFirst(",", "").trim().split(",");
	        		for (int i = 0; i <reviewData1.length; i++)
	        		{
			        	if (reviewData1[i].contains("text:"))
			        	{
			        		String reviewTemp = new String();
			        		reviewTemp = reviewTemp.concat(reviewData1[i]);
			        		while (!reviewData1[i + 1].contains("type: review"))
			        		{ 
			        			reviewTemp = reviewTemp.concat(",").concat(reviewData1[i + 1]);
			        			i++;
			        		}
			        		reviewData.add(reviewTemp);
			        	}
			        	else
			        	{
			        		reviewData.add(reviewData1[i]);
			        	}
		        	}
		        	for (int i = 0; i < reviewData.size(); i++)
		        	{
		        		System.out.println("\n" + i + ":" + reviewData.get(i));			        					        			
		       		}

		     		HbaseBuildInputData.buildPutData(reviewData, "Review Object");
		       	}
		        
		        else if (line.contains("type: business")) 
		        {
		        	String business[] = line.split(",");
		        	ArrayList <String> businessData = new ArrayList<String>();
		        	int loopCountExtraCommaIndicator = 0;
		        	
		        	for (int i = 0; i < business.length - loopCountExtraCommaIndicator; i++)
	        		{
		        		if (business[i].contains("full_address:"))
		        		{
		        			String businessTemp = new String();
		        			while (!business[i + 1].contains("type: business") && !business[i + 1].contains("schools: [")
		        					&& !business[i + 1].contains("business_id:") && !business[i + 1].contains("open:")
		        					&& !business[i + 1].contains("categories: [") && !business[i + 1].contains("photo_url:"))	
		        			{
		        				businessTemp = businessTemp.concat(business[i]).concat(",");
		        				i++;
		        			}
		        			businessTemp = businessTemp.concat(business[i]);
		        			businessData.add(businessTemp);
		        		}

		        		else if (!business[i].contains("[]"))
		        		{
		        			if (!business[i].contains("["))
		        			{	        				
		        				if (business[i].contains(": ") && i == business.length- 1)
		        				{
		        					businessData.add(business[i]);     				
		        				}
		        				else if (business[i].contains(": ") && business[i + 1].contains(": "))
		        				{
		        					businessData.add(business[i]);     				
		        				}
		        				else
		        				{
		        					loopCountExtraCommaIndicator++;
		        					business[i] = business[i].concat(business[i + 1]);
		        					if (business[i].contains(": ") && business[i + 1].contains(": "))
				        			{
				        				businessData.add(business[i]);     				
				        			}
		        					else
		        					{
		        						i--;
		        					}	        					
		        					for (int j = i + 1; j < business.length - 1; j++)
		        					{
		        						business[j] = business[j + 1];
		        					}        					 
		        				}
		        			}
		        			else if (business[i].contains("["))
		        			{
		        				if (business[i].contains("]"))
		        				{
		        					businessData.add(business[i]);
		        				}
		        				else 
		        				{
		        					String businessTemp = new String();
		        					while (!business[i].contains("]"))
		        					{	        						
		        						businessTemp = businessTemp.concat(business[i]).concat(",");
		        						i++;		        
		        					}
		        					businessTemp = businessTemp.concat(business[i]);
		        					businessData.add(businessTemp);
		        				}
		        			}		        	
		        		}
	        		}
		        	
		        	for (int i = 0; i < businessData.size(); i++)
	        		{
	        			System.out.println("\n " + i + ":" + businessData.get(i));			        					        			
	        		}
		        	
		        	HbaseBuildInputData.buildPutData(businessData, "Business Object");
		        }
		        else
		        {
		        	throw new UserExceptions("A different object!");
		        }
		    }
		} 
		catch (UserExceptions e)
		{
			e.printStackTrace();
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
}
