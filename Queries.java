import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;

/**
 * Queries class  contains different queries to fetch data from the data store.
 */
public class Queries {
	
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
		query = "SELECT * FROM businessobjects LIMIT 1;";
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of query viewContentsBusiness:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 2: Total reviews in the data set.
	 */
	public static void totalReviews()
	{
		query = "SELECT SUM(review_count) AS totalreviews FROM businessobjects;";
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of query totalReviews:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 3: Count total number of reviews for particular state in Yelp business data
	 */
	public static void countReviewsState()
	{
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
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of query countReviewsState:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 4: Average number of reviews per business star rating
	 */
	public static void averageReviewsBusiness()
	{
		//....
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of baverageReviewsBusiness:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 5: Businesses with high review counts (> 1000)
	 */
	public static void businessHighReviewCount()
	{
		query = "SELECT name, state, city, review_count FROM businessobjects WHERE review_count > 1000 ALLOW FILTERING;";
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of businessHighReviewCount:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 6: Number of restaurants in the data set
	 */
	public static void numberRestaurants()
	{
		String queryCreateIndexCategories = "CREATE INDEX categories_idx ON businessobjects(categories);";
		String queryDropIndexCategories = "DROP INDEX categories_idx";
		query = "SELECT COUNT(*) AS TotalRestaurants FROM businessobjects WHERE categories CONTAINS 'Restaurants';";
		
		result = CassandraDBConnect.session.execute(queryCreateIndexCategories);
		System.out.println("Index created on categories columnn of yelp business data" + result.all());
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of numberRestaurants:");
		System.out.println(result.all());
		
		result = CassandraDBConnect.session.execute(queryDropIndexCategories);
		System.out.println("Index droped on categories columnn of yelp business data" + result.all());	
	}
	
	/**
	 * Query 7: Restaurants in number of reviews
	 */
	public static void listRestaurants()
	{
		query = "SELECT name, state, city, review_count FROM businessobjects WHERE categories CONTAINS 'Restaurants';";
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of listRestaurants:");
		System.out.println(result.all());
	}
	
	/**
	 * Query 8: Top restaurants in number of listed categories
	 */
	/*public static void ()
	{
		
	}*/
	
	/**
	 * Query 9: First categories in number of review counts
	 */
	public static void firstCategoriesReviewCount()
	{
		List<List> categories = new ArrayList();
		List<String> firstCategory = new ArrayList();
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
	 * Query 10: Businesses with cool rated reviews > 2000
	 */
	public static void businessesCoolReviews()
	{
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
	
	/**
	 * Query 11: Creating Materialized view of userobjects
	 */
	public static void viewUserObjects()
	{
		queryView = "CREATE MATERIALIZED VIEW IF NOT EXISTS viewUserObjects AS SELECT name, type, review_count FROM userobjects WHERE "
				+ "review_count IS NOT NULL PRIMARY KEY (review_count,user_id);";
		
		query = "SELECT name, review_count, user_id FROM viewUserObjects WHERE review_count = 23 ORDER BY user_id DESC LIMIT 5";
		
		result = CassandraDBConnect.session.execute(queryView);
		System.out.println("" + result.all());
		
		result = CassandraDBConnect.session.execute(query);
		System.out.println("Result of viewUserObjects:");

		
		while(result.iterator().hasNext())
		{
			System.out.println(result.one().toString());
		}
	}		
}
