# Description of the project: 
This project analyzed relational and non-relational database paradigms within the context of the Advanced Database Course. 
Through schema design, indexing, and query optimizations, this project provided an exploration of database structures and performance enhancements across SQLite and MongoDB, highlighting the nuanced differences between relational and non-relational approaches. By creating better indexing, query times were improved by 95%, showcasing the significant impact of optimization techniques on database performance.


Using the same data, we created two DataBases:
## NoSQL DataBase Schema:

![NoSQL](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/f8748330-be1f-4128-8760-a9a165911a86)

## MongoDB Indexing

![image](https://github.com/user-attachments/assets/8612c918-9473-49b4-b6ac-9c2a40ba74fb)

In this example, we index all of the ratings of books.
Instead of searching for the whole DataBase books, our search query will only examine the Book_Rating Index.

Advantages:
- Indexing allows us to focus on specific subsets of documents, significantly improving query performance for targeted searches.
- For collections with a large number of documents, sorting becomes faster and more efficient when indexes are used.

Disadvantages:
- Any updates to the collection require updating the corresponding indexes, which can add computational and storage overhead. This challenge becomes more significant in collections with a large number of indexes, as the maintenance cost increases proportionally.


### Indexing that we used 

In mongoDB there are multiple types of indexing and the two we think are the most beneficial
are the compound index and the text index. 

Firstly, we used the compound index in order to combine documents. 
Then we used Text indexes to remove certain words of movie names, from every collection, and we added the used keys in the queries, having almost no difference to if they’re ascending or descending.

### NoSQL Time Improvement:

![imagem](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/e930c17c-ea11-4b93-bf1a-6736930e586b)

The times shown refer to the simple and complex select queries and the times are shown in seconds.

![BDA_Improvment](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/94f1e44a-269d-4140-9430-eb9d3d4cbaed)

The time taken for the Select operation 'sel_comp1' in the Database, which utilized a Compound Index, showed a `95% improvement.`


## SQL DataBase Schema:

![SQL](https://github.com/ricasbp/DisneyMoviesDB/assets/59062659/7b6c0f7a-cd77-4778-b11b-a751b11af7ec)

There was no significant time improvement in the SQL DataBase because the DB was too small for a remarkable time decrease.
