### Spark Task #1 : RDD Operations

Using the Orders Data set ingested:

1.1 Find customer(s) who have ordered more that 10 times
1.2 Find the orderDate of the 'In Process' order for customerNumber 124
1.3 What is the total payment made (all orders) by customerNumber 124?
1.4 Which customer has the highest amount for one order?
1.5 Create a list of - customerNumber, orderDate, paymentDate of orders places in 2013
1.6 Find totals by customers for the top 10 customer
1.7 Plot results on a graph

### Spark Task #2 : Dataframes and SparkSQL

Using orders, orderdetails and products

2.1 Find orders that were shipped late to the customer
2.2 List orders where total amount exceeds $250
2.3 Find the productCode per order that is most expensive
2.4 Write a list of customers (include demographic information) whose order were late (point 1 above). This list should have an additional column called Gift Card Amount. Populate Gift Card Amount as follows: If total order exceeds $100 then Gift Card Amount=$50 else Gift Card Amount=$25, Use Parquet format for output.
2.5 Can you output the list above in 1 single CSV file?

### Spark Task #3 : Create Application

Using orders, orderdetails and products and code from previous assignment create a Spark Application that:

3.1 Create a list of customers (include demographic information) whose order were late (point 1 above). This list should have an additional column called Gift Card Amount. Populate Gift Card Amount as follows: If total order exceeds $100 then Gift Card Amount=$50 else Gift Card Amount=$25
3.2 Create a HIVE table for the list above

### Spark Task #4 : Spark Stream on Twitter Data

4.1 Complete the Twitter Hashtag example using your own Twitter Developer Account