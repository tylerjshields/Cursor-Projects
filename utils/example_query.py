from snowflake_connector import get_data, execute_query
import pandas as pd
import matplotlib.pyplot as plt

# Example query - replace with your actual query
query = """
SELECT 
    DATE_TRUNC('week', sent_at_date) as week,
    COUNT(*) as notification_count,
    COUNT(DISTINCT consumer_id) as unique_consumers
FROM proddb.public.nvg_notif_metrics_base
WHERE sent_at_date BETWEEN '2025-01-01' AND '2025-02-01'
GROUP BY week
ORDER BY week
"""

# Run the query and get the results
print("Executing query...")
results = get_data(query)

# Display the results
print("\nQuery Results:")
print(results)

# Example of further data processing
if not results.empty:
    # Plot the results
    plt.figure(figsize=(10, 6))
    plt.plot(results['week'], results['notification_count'], marker='o', label='Total Notifications')
    plt.plot(results['week'], results['unique_consumers'], marker='x', label='Unique Consumers')
    plt.title('Notification Metrics by Week')
    plt.xlabel('Week')
    plt.ylabel('Count')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('notification_metrics.png')
    print("\nPlot saved as 'notification_metrics.png'")
    
    # Example of saving results to a CSV file
    results.to_csv('notification_metrics.csv', index=False)
    print("Data saved to 'notification_metrics.csv'")
else:
    print("No results returned from the query.") 