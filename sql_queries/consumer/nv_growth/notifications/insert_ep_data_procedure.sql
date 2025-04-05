-- Create a stored procedure to execute EP queries and insert results
CREATE OR REPLACE PROCEDURE proddb.tylershields.insert_ep_consumer_data(
    start_date DATE, 
    end_date DATE
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Set up variables to track progress
    var programs_processed = 0;
    var success_count = 0;
    var error_count = 0;
    var error_messages = [];
    var rows_inserted = 0;
    
    try {
        // Format dates as YYYY-MM-DD strings for SQL
        function formatDateForSnowflake(dateObj) {
            if (typeof dateObj === 'string') {
                // If it's already a string, just wrap it in quotes
                return "'" + dateObj + "'";
            }
            
            // Otherwise extract date components
            var yyyy = dateObj.getFullYear();
            var mm = String(dateObj.getMonth() + 1).padStart(2, '0');
            var dd = String(dateObj.getDate()).padStart(2, '0');
            return "'" + yyyy + "-" + mm + "-" + dd + "'";
        }
        
        // Format the date parameters correctly for SQL
        var start_date_str = formatDateForSnowflake(START_DATE);
        var end_date_str = formatDateForSnowflake(END_DATE);
        
        // First get the list of programs with their queries using the unified query approach
        var get_programs_sql = `
            WITH recent_runs AS (
                SELECT 
                    program_name,
                    MAX(run_at) AS latest_run_date
                FROM growth_service_prod.public.engagement_program_run_results
                GROUP BY program_name
            ),
            
            ep_list AS (
                SELECT DISTINCT ep_name
                FROM proddb.public.nv_channels_notif_index
                WHERE ep_name IS NOT NULL
            ),
            
            program_queries AS (
                SELECT 
                    e.name AS program_name,
                    e.query AS original_query,
                    r.latest_run_date
                FROM growth_service_prod.public.engagement_program e
                JOIN ep_list n ON e.name = n.ep_name
                LEFT JOIN recent_runs r ON e.name = r.program_name
                WHERE e.query IS NOT NULL
            )
            
            SELECT 
                program_name,
                original_query,
                latest_run_date
            FROM program_queries
        `;
        
        // Execute the query to get program details
        var programs_stmt = snowflake.createStatement({sqlText: get_programs_sql});
        var programs_result = programs_stmt.execute();
        
        // Process each program
        while (programs_result.next()) {
            programs_processed++;
            var program_name = programs_result.getColumnValue("PROGRAM_NAME");
            var original_query = programs_result.getColumnValue("ORIGINAL_QUERY");
            var latest_run_date = programs_result.getColumnValue("LATEST_RUN_DATE");
            
            // Simply replace date parameters directly, without using to_date()
            // This is cleaner because we're already passing DATE types to the procedure
            var processed_query = original_query.replace(/;/g, ""); // Remove any semicolons
            
            // Direct replacements with properly formatted date strings
            processed_query = processed_query.replace(/:START_DATE/g, start_date_str);
            processed_query = processed_query.replace(/:END_DATE/g, end_date_str);
            
            // Construct insert statement to execute the dynamic query and insert results
            var insert_sql = `
                INSERT INTO proddb.public.nvg_channels_ep_daily (ds, ep_name, consumer_id)
                SELECT 
                    ${end_date_str}::DATE AS ds,
                    '${program_name}' AS ep_name,
                    consumer_id
                FROM (${processed_query})
            `;
            
            try {
                // Execute the insert statement
                var insert_stmt = snowflake.createStatement({sqlText: insert_sql});
                var insert_result = insert_stmt.execute();
                
                // Get number of rows inserted
                var get_rows_sql = "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))";
                var rows_stmt = snowflake.createStatement({sqlText: get_rows_sql});
                var rows_result = rows_stmt.execute();
                
                if (rows_result.next()) {
                    var number_of_rows = rows_result.getColumnValue("number of rows inserted");
                    rows_inserted += number_of_rows;
                }
                
                success_count++;
            } catch (err) {
                error_count++;
                error_messages.push(`Error processing EP '${program_name}': ${err.message}`);
            }
        }
        
        // Return summary
        return `Processed ${programs_processed} engagement programs.
                Successful inserts: ${success_count}
                Failed inserts: ${error_count}
                Total rows inserted: ${rows_inserted}
                ${error_messages.length > 0 ? "Errors: " + error_messages.join("; ") : ""}`;
                
    } catch (main_err) {
        return `Failed to execute procedure: ${main_err.message}`;
    }
$$;

-- USAGE:
-- CALL proddb.tylershields.insert_ep_consumer_data(CURRENT_DATE(), CURRENT_DATE()); 