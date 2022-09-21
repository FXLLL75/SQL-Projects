--We are running an experiment at an item-level, which means all users who visit will see the same page, but the layout of different item pages may differ.
--Compare this table to the assignment events we captured for user_level_testing.
--Does this table have everything we need to compute metrics like 30-day view-binary?

SELECT 
  * 
FROM 
  dsv1069.final_assignments_qa;
  
SELECT * FROM dsv1069.final_assignments;
  
--ANSWER : No, we need the date and time of assignment in order to compute some of our metrics like 30-day view-binary.




/* Let's write a query and table creation statement to make final_assignments_qa look like the final_assignments table. */

WITH final_assignments_qa_fixed
AS 
(SELECT item_id,
       test_a AS test_assignment,
       'test_a' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa
UNION ALL
SELECT item_id,
       test_b AS test_assignment,
       'test_b' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa
UNION ALL
SELECT item_id,
       test_c AS test_assignment,
       'test_c' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa
UNION ALL
SELECT item_id,
       test_d AS test_assignment,
       'test_d' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa
UNION ALL
SELECT item_id,
       test_e AS test_assignment,
       'test_e' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa
UNION ALL
SELECT item_id,
       test_f AS test_assignment,
       'test_f' AS test_number,
       CAST('2013-01-05 00:00:00' AS timestamp) AS test_start_date
FROM dsv1069.final_assignments_qa)
SELECT * FROM final_assignments_qa_fixed



/* Let's use the final_assignments table to calculate the order binary for the 30 day window after the test assignment for item_test_2 (includung the day the test started)*/
SELECT order_binary.test_assignment,
       COUNT(DISTINCT order_binary.item_id) AS num_orders,
       SUM(order_binary.order_binary_30d) AS sum_orders_bin_30d
FROM
  (SELECT assignments.item_id,
          assignments.test_assignment,
          MAX(CASE
                  WHEN (orders.created_at > assignments.test_start_date
                        AND DATE_PART('day', created_at - test_start_date) <= 30) THEN 1
                  ELSE 0
              END) AS order_binary_30d
   FROM dsv1069.final_assignments AS assignments
   LEFT JOIN dsv1069.orders AS orders
     ON assignments.item_id = orders.item_id
   WHERE assignments.test_number = 'item_test_2'
   GROUP BY assignments.item_id,
            assignments.test_assignment) AS order_binary
GROUP BY order_binary.test_assignment



/* We will use the final_assignments table to calculate the view binary and average views for the 30 day window, after the test assignment for item_test_2. (including the day the test started)*/
SELECT view_binary.test_assignment,
       COUNT(DISTINCT view_binary.item_id) AS num_views,
       SUM(view_binary.view_bin_30d) AS sum_view_bin_30d,
       AVG(view_binary.view_bin_30d) AS avg_view_bin_30d
FROM
  (SELECT assignments.item_id,
          assignments.test_assignment,
          MAX(CASE
                  WHEN (views.event_time > assignments.test_start_date
                        AND DATE_PART('day', event_time - test_start_date) <= 30) THEN 1
                  ELSE 0
              END) AS view_bin_30d
   FROM dsv1069.final_assignments AS assignments
   LEFT JOIN dsv1069.view_item_events AS views
     ON assignments.item_id=views.item_id
   WHERE assignments.test_number='item_test_2'
   GROUP BY assignments.item_id,
            assignments.test_assignment
   ORDER BY item_id) AS view_binary
GROUP BY view_binary.test_assignment


/* We used ABBA (https://thumbtack.github.io/abba/demo/abba.html) to compute the lifts in metrics and the p-values for the binary metrics ( 30 day order binary and 30 day view binary) using a interval 95% confidence.*/

--For orders_bin: lift is -14% – 12% (-1%) and pval is 0.88
--For view_bin:   lift is -1.6% – 6.1% (2.3%) and pval is 0.25
-- This means that for test_2, there was no signficant variation of the number of views and orders between control and treatment.
