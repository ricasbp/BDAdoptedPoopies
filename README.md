# Description of Project: 
This project rigorously analyzed relational and non-relational database paradigms within the context of Advanced Databases.

Through meticulous schema design, indexing, and query optimizations, this project provided a comprehensive exploration of database structures and performance enhancements across SQLite and MongoDB, highlighting the nuanced differences between relational and non-relational approaches.

Using the same data, we created two DataBases:
## NoSQL DataBase Schema:

![NoSQL](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/f8748330-be1f-4128-8760-a9a165911a86)

In mongoDB there are more types of indexing and the two we think were the most beneficial
are the compound index and the text index. Firstly, we used the compound index in order to
combine documents. We used Text indexes to remove certain words of movie names, from
every collection, and we added the used keys in the queries, having almost no difference to
if they’re ascending or descending.

### NoSQL Time Improvement:

![imagem](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/e930c17c-ea11-4b93-bf1a-6736930e586b)


The times shown refer to the simple and complex select queries and the times are shown in seconds.



## SQL DataBase Schema:

![SQL](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/7b6c0f7a-cd77-4778-b11b-a751b11af7ec)

There was no significant time improvement in the SQL DataBase because the DB was too small.
