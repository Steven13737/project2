import click
from google.cloud import bigquery

uni1 = 'yq2247' # Your uni
uni2 = 'gq2138' # Partner's uni. If you don't have a partner, put None

# Test function
def testquery(client):
    q = """select * from dataset.Graph` limit 3"""
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
    q1 = """
       select id, text from `w4111-columbia.graph.tweets`where text like "%going live%" and text like "%www.twitch%"
        """
    job = client.query(q1)
    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q2 = """
       select substr(create_time,1,3),avg(like_num) from `w4111-columbia.graph.tweets` group by substr(create_time,1,3) order by avg(like_num) DESC limit 1
        """
    job = client.query(q2)
    results = job.result()
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    q3 = """
       CREATE TABLE if not exists dataset.Graph AS \
       select distinct twitter_username as src,REGEXP_EXTRACT(text, r"@+([a-zA-Z0-9-]+)") as dst\
         from  (select twitter_username, text from `w4111-columbia.graph.tweets` where text like "%@%") \
       """
    job = client.query(q3)
    results = job.result()
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q4 = """
    select a.indegree, b.outdegree \
    from \
      (select count(src) as indegree from dataset.Graph group by dst order by count(src) DESC limit 1) as a,
      (select count(dst) as outdegree from dataset.Graph group by src order by count(dst) DESC limit 1) as b
      """
    job = client.query(q4)
    results = job.result()
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
    q5 = """
    create table if not exists dataset.unpopular as (\
    select distinct w.twitter_username  from `w4111-columbia.graph.tweets` as w,  (select dst, count(src) as indegree from dataset.Graph group by dst ) as i\
            where w.like_num < (select avg(like_num) from `w4111-columbia.graph.tweets`) \
                and w.twitter_username = i.dst and \
                i.indegree < (select avg(indegree) from (select dst, count(src) as indegree from dataset.Graph group by dst ))
                )
      """
    q51 = """select * from dataset.unpopular limit 1"""
    #Popular
    q52 = """
    create table if not exists dataset.popular as (\
    select distinct w.twitter_username  from `w4111-columbia.graph.tweets` as w,  (select dst, count(src) as indegree from dataset.Graph group by dst ) as i\
            where w.like_num >= (select avg(like_num) from `w4111-columbia.graph.tweets`) \
                and w.twitter_username = i.dst and \
                i.indegree >= (select avg(indegree) from (select dst, count(src) as indegree from dataset.Graph group by dst ))
                )
      """
    q53 = """select * from dataset.unpopular limit 3""" 
    # distinct ?
    q54 = """
        select count(distinct g.src)/(select count(twitter_username) from dataset.unpopular) from dataset.unpopular as u, dataset.popular as p, dataset.Graph as g \
        where u.twitter_username = g.src and p.twitter_username = g.dst 
        """
#    job = client.query(q5)
#    job = client.query(q51)
#    job = client.query(q52)
#    job = client.query(q53)
    job = client.query(q54)
    results = job.result()
    return list(results)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
    q6 ="""
    with number as (
    select distinct g1.src, g2.src, g3.src \
    from dataset.Graph as g1, dataset.Graph as g2, dataset.Graph as g3 \
    where g1.dst = g2.src and g2.dst = g3.src and g3.dst = g1.src
    )
    select count(*) from number
    """
    job = client.query(q6)
    results = job.result()
    return list(results)

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client,start):
    #Out Degree
    q71 = "create or replace table dataset.outdegree as (select count(dst) as outdegree from dataset.Graph group by src)"
    
    #Initialize
    q72 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 1/(select count (distinct twitter_username) from `w4111-columbia.graph.tweets`) as pagerank
        """.format(start=start)

#    job = client.query(q71)
    job = client.query(q72)
    results = job.result()
    return list(results)


# Do not edit this function. This is for helping you develop your own iterative PageRank algorithm.
def bfs(client, start, n_iter):

    # You should replace dataset.bfs_graph with your dataset name and table name.
    q1 = """
        CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
        """
    q2 = """
        INSERT INTO dataset.bfs_graph(src, dst) VALUES
        ('A', 'B'),
        ('A', 'E'),
        ('B', 'C'),
        ('C', 'D'),
        ('E', 'F'),
        ('F', 'D'),
        ('A', 'F'),
        ('B', 'E'),
        ('B', 'F'),
        ('A', 'G'),
        ('B', 'G'),
        ('F', 'G'),
        ('H', 'A'),
        ('G', 'H'),
        ('H', 'C'),
        ('H', 'D'),
        ('E', 'H'),
        ('F', 'H');
        """

    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()

    # You should replace dataset.distances with your dataset name and table name. 
    q3 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 0 as distance
        """.format(start=start)
    job = client.query(q3)
    # Result will be empty, but calling makes the code wait for the query to complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i+1))
        q1 = """
        INSERT INTO dataset.distances(node, distance)
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.distances
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.distances
                )
            """.format(
                curr_distance=i,
                next_distance=i+1
            )
        job = client.query(q1)
        results = job.result()
        # print(results)


# Do not edit this function. You can use this function to see how to store tables using BigQuery.
def save_table():
    client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = True
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('test')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
    client = bigquery.Client.from_service_account_json(pathtocred)

    #funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    funcs_to_test = [q7]
    for func in funcs_to_test:
        rows = func(client,'Askren')
        print ("\n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
  main()
