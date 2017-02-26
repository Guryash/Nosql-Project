import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;

/**
 * Queries class  contains different queries to fetch data from the data store.
 */
public class CassandraQueries 
{	
	static Stopwatch timer = new Stopwatch();
	static String query = null;
	static String queryView = null;
	static String queryFunction = null;
	static String queryAggregate = null;
	static String queryFinalFunction = null;
	static String subQuery = null;
	static ResultSet result = null;
	
	/**
	 * Query 1: View the contents of the Yelp business data.
	 */
	public static void viewContentsBusiness()
	{
		System.out.println("\n\n----------Query 1----------");
		
		query = "SELECT * FROM businessobjects LIMIT 1;";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, viewContentsBusiness: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of query viewContentsBusiness:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 2: Total reviews in the business dataset.
	 */
	public static void totalReviews()
	{
		System.out.println("\n\n----------Query 2----------");
		
		query = "SELECT SUM(review_count) AS totalreviews FROM businessobjects;";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, totalReviews: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of query totalReviews:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 3: Count total number of reviews for particular state in Yelp business data.
	 */
	public static void countReviewsState()
	{
		System.out.println("\n\n----------Query 3----------");
		
		queryFunction = "CREATE OR REPLACE FUNCTION review_states(state_review map<text, int>,  state text) "
				+ "CALLED ON NULL INPUT RETURNS map<text, int> LANGUAGE JAVA AS $$ Integer count = (Integer) "
				+ "state_review.get(state); if (count == null) count = 1; else count++; state_review.put(state, count); "
				+ "return state_review; $$;";		
		queryAggregate = "CREATE OR REPLACE AGGREGATE review_states(text) SFUNC review_states STYPE map<text, int>"
				+ "INITCOND {};";		
		query = "SELECT review_states(state) FROM businessobjects;";		
		
		result = CassandraDBConnect.session.execute(queryFunction);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(queryAggregate);
		System.out.println("" + result.all());
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, countReviewsState: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of query countReviewsState:");
		System.out.println(result.all());
	}
		
	/**
	 * Query 4: Businesses with high review counts (> 1000).
	 */
	public static void businessHighReviewCount()
	{
		System.out.println("\n\n----------Query 4----------");
		
		query = "SELECT name, state, city, review_count FROM businessobjects WHERE review_count > 1000 ALLOW FILTERING;";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, businessHighReviewCount: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of businessHighReviewCount:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 5: Number of restaurants in the business dataset.
	 */
	public static void numberRestaurants()
	{
		System.out.println("\n\n----------Query 5----------");
		
		String queryCreateIndexCategories = "CREATE INDEX categories_idx ON businessobjects(categories);";
		
		query = "SELECT COUNT(*) AS TotalRestaurants FROM businessobjects WHERE categories CONTAINS 'Restaurants';";
		
		result = CassandraDBConnect.session.execute(queryCreateIndexCategories);
		System.out.println("Index created on categories columnn of yelp business data" + result.all());
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, numberRestaurants: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of numberRestaurants:");
		System.out.println(result.all());	
	}
	
	/**
	 * Query 6: Restaurants in number of reviews.
	 */
	public static void listRestaurants()
	{
		System.out.println("\n\n----------Query 6----------");
		
		//String queryDropIndexCategories = "DROP INDEX categories_idx;";
		
		query = "SELECT name, state, city, review_count FROM businessobjects WHERE categories CONTAINS 'Restaurants';";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, listRestaurants: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of listRestaurants:");
		System.out.println(result.all());
		
		result = CassandraDBConnect.session.execute(queryDropIndexCategories);
		System.out.println("Index droped on categories columnn of yelp business data" + result.all());
	}
		
	/**
	 * Query 7: View the contents of the review data.
	 */
	public static void viewContentsReview()
	{
		System.out.println("\n\n----------Query 7----------");
		
		query = "SELECT * FROM reviewobjects LIMIT 1;";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, viewContentsReview: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of query viewContentsReview:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 8: Creating Materialized view of userobjects.
	 */
	public static void viewUserObjects()
	{
		System.out.println("\n\n----------Query 8----------");
		
		queryView = "CREATE MATERIALIZED VIEW IF NOT EXISTS viewUserObjects AS SELECT name, type, review_count FROM userobjects WHERE "
				+ "review_count IS NOT NULL PRIMARY KEY (review_count,user_id);";
		
		timer.start();
		result = CassandraDBConnect.session.execute(queryView);
		timer.stop();
				
		System.out.println("Time elapsed to execute a query, viewUserObjects: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of viewUserObjects:" + result.all());
	}
	
	/**
	 * Query 9: Reading values from Materialized view of userobjects.
	 */
	public static void selectViewUserObjects()
	{
		System.out.println("\n\n----------Query 9----------");
				
		query = "SELECT name, review_count, user_id FROM viewUserObjects WHERE review_count = 23 ORDER BY user_id DESC LIMIT 5;";
		
		timer.start();
		result = CassandraDBConnect.session.execute(query);
		timer.stop();
		System.out.println("Time elapsed to execute a query, viewUserObjects: " + timer.elapsed(TimeUnit.MILLISECONDS) + " milliseconds.");
		timer.reset();
		System.out.println("Result of viewUserObjects:");
		
		while(result.iterator().hasNext())
		{
			System.out.println(result.one().toString());
		}
	}	
	
	/**
	 * Query 10: First categories in number of review counts.
	 */
	public static void firstCategoriesReviewCount()
	{
		System.out.println("\n\n----------Query 10----------");

		List<List> categories = new ArrayList<List>();
		List<String> firstCategory = new ArrayList<String>();
		String queryCategories = "SELECT categories FROM businessobjects";
		ResultSet resultCategories = CassandraDBConnect.session.execute(queryCategories);
        int count = 0;
		
        while(resultCategories.iterator().hasNext())
		{
			categories.add(resultCategories.one().getList(0, String.class));			
			System.out.println(categories.get(count));
			count++;
		}
		
		for (List categoriesList: categories)
		{
			if (!categoriesList.isEmpty())
			{
				firstCategory.add(categoriesList.get(0).toString());
			}
			
		}
		
		queryFunction = "CREATE OR REPLACE FUNCTION first_category(count_category map<text, int>,  category text) CALLED "
				+ "ON NULL INPUT RETURNS map<text, int> LANGUAGE JAVA AS $$ Integer count = count_category.get(category); "
				+ "if (count == null) count = 1; else count++; count_category.put(category, count); return count_category; $$;";
		
		queryAggregate = "CREATE OR REPLACE AGGREGATE first_category(text) SFUNC first_category STYPE map<text, int>"
				+ "initcond {};";
		
		//doesn't work
		query = "SELECT first_category(firstcategory) FROM businessobjects;";
		
		result = CassandraDBConnect.session.execute(queryFunction);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(queryAggregate);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of firstCategoriesReviewCount:");
		System.out.println(result.all());
	}
		
	/**
	 * Query 11: Businesses with cool rated reviews > 2000.
	 */
	public static void businessesCoolReviews()
	{
		System.out.println("\n\n----------Query 11---------");

		queryFunction = "CREATE OR REPLACE FUNCTION sum_coolReviews(sum_cool map<text, int>, business_id text, cool int) CALLED "
				+ "ON NULL INPUT RETURNS map<text, int> LANGUAGE JAVA AS $$ if(sum_cool.containsKey(business_id)) { "
				+ "sum_cool.put(business_id, sum_cool.get(business_id) + cool); } else { sum_cool.put(business_id, cool); } return "
				+ "sum_cool; $$;";
		
		//doesn't work 
		queryFinalFunction = "CREATE OR REPLACE FUNCTION final_sum_coolReviews(sum_cool map<text, int>) RETURNS NULL ON NULL INPUT "
				+ "RETURNS text LANGUAGE JAVA AS $$ if(sum_cool.getValue > 2000) { return sum_cool.getKey(); } ; else { return null; } $$;";
		
		queryAggregate = "CREATE OR REPLACE AGGREGATE sum_coolReviews(text, int) SFUNC sum_coolReviews STYPE map<text, int> FINALFUNC "
				+ "final_sum_coolReviews INITCOND {};";
		
		result = CassandraDBConnect.session.execute(queryFunction);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(queryFinalFunction);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(queryAggregate);
		System.out.println("" + result.all());
		
	}
}
