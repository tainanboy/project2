import click
from google.cloud import bigquery

uni1 = 'ho2271' # Your uni
uni2 = 'sl4401' # Partner's uni. If you don't have a partner, put None

# Test function
def testquery(client):
    q = """select * from `w4111-columbia.graph.tweets` limit 3"""
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
    q = """
    select t1.id, t1.text 
    from `w4111-columbia.graph.tweets` t1 
    where t1.text like '%going live%' 
    INTERSECT DISTINCT
    select t2.id, t2.text 
    from `w4111-columbia.graph.tweets` t2 
    where t2.text like '%twitch.com%' 
    """
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q = """
    with temp as (
    select SUBSTR(create_time,0,3) as day, like_num as like_num 
    from `w4111-columbia.graph.tweets`)
    select day as day, avg(like_num) as avg_likes
    from temp
    group by day
    order by avg(like_num) desc
    limit 1;
    """
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    q = """
    create or replace table dataset.GRAPH as 
    with temp as (
    select t1.twitter_username as src, t1.text as text 
    from `w4111-columbia.graph.tweets` t1
    where t1.text like '%@%')
    select distinct src as src, REGEXP_EXTRACT(text, r"@([A-Za-z0-9_]+)") as dst
    from temp 
    where src != REGEXP_EXTRACT(text, r"@([A-Za-z0-9_]+)")
    """
    job = client.query(q)
    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q = """
    with max_outdegree as(
    select src as src 
    from dataset.GRAPH 
    group by src
    order by count(*) desc 
    limit 1
    )
    select tmp.dst as max_indegree, src as max_outdegree
    from max_outdegree, (select dst as dst from dataset.GRAPH 
    group by dst
    order by count(*) desc 
    limit 1) as tmp
    """
    job = client.query(q)
    
    # waits for query to execute and return
    results = job.result()

    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
    q = """
    create or replace table dataset.indegree as 
    select dst as dst, count(*) as indegree
    from dataset.GRAPH 
    group by dst;
    """
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()

    q2= """
    create or replace table dataset.unpopular as 
    select i.dst as unpopUser
    from dataset.indegree i, `w4111-columbia.graph.tweets` t
    where i.indegree < (select avg(i1.indegree) from dataset.indegree i1) 
    and t.like_num < (select avg(t1.like_num) from `w4111-columbia.graph.tweets` t1) 
    and t.twitter_username = i.dst;
    """
    job2 = client.query(q2)

    # waits for query to execute and return
    results2 = job2.result()

    q3= """
    create or replace table dataset.popular as 
    select i.dst as popUser
    from dataset.indegree i, `w4111-columbia.graph.tweets` t
    where i.indegree >= (select avg(i1.indegree) from dataset.indegree i1) 
    and t.like_num >= (select avg(t1.like_num) from `w4111-columbia.graph.tweets` t1) 
    and t.twitter_username = i.dst
    """
    job3 = client.query(q3)

    # waits for query to execute and return
    results3 = job3.result()

    # unpopular user mentions popular users
    q4= """
    with temp as (
    select count(*) as num
    from dataset.GRAPH g1
    where g1.dst in (select popUser from dataset.popular)
    and g1.src in (select unpopUser from dataset.unpopular)
    )
    select temp.num/tmp.num2 as popular_unpopular
    from temp,  (select count(*) as num2 from dataset.GRAPH g2
    where g2.src in (select unpopUser from dataset.unpopular)
    ) as tmp
    """
    job4 = client.query(q4)
    # waits for query to execute and return
    results4 = job4.result()

    return list(results4)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
    q = """
    select (count(*)/3) as no_of_triangles
    from dataset.GRAPH g1, dataset.GRAPH g2, dataset.GRAPH g3
    where g1.dst = g2.src and g2.dst = g3.src and g3.dst = g1.src
    """
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank.
def q7(client):
    n_iter = 20
    # create PR table: node, PR
    # create or replace table dataset.PR as
    q1 = """
    create or replace table dataset.PR as 
    with node as (
    select g1.src as node
    from dataset.GRAPH g1
    union distinct
    select g2.dst as node
    from dataset.GRAPH g2
    )
    select n1.node as node, 1/tmp.num2 as pr
    from node n1, (select count(*) as num2 from node n2) as tmp
    """
    # create edge table: src, dst, carry
    q2 = """
    create or replace table dataset.edge as 
    with tmp as (
    select g1.src as src, g1.dst as dst
    from dataset.GRAPH g1
    )
    select tmp.src as src, tmp.dst as dst, null as carry
    from tmp
    """
    # create outdegree: node, outdegree
    q3 = """
    create or replace table dataset.outdegree as 
    select src as node, count(*) as outdegree
    from dataset.GRAPH 
    group by src;
    """

    q4 = """
    create or replace table dataset.pr_outdegree as 
    select p.node as node, p.pr as pr, o.outdegree as outdegree, p.pr/o.outdegree as carry
    from dataset.PR p, dataset.outdegree o
    where p.node = o.node
    """
    
    q5 = """
    create or replace table dataset.edge1 as 
    SELECT src, dst, CAST(carry AS FLOAT64) as carry
    FROM dataset.edge
    """
    
    # waits for query to execute and return
    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()
    job = client.query(q5)
    results = job.result()
    job = client.query(q3)
    results = job.result()
    job = client.query(q4)
    results = job.result()
    # iter update
    for i in range(n_iter):
        print("Step %d..." % (i+1))
        # update edge
        q6 = """
        update dataset.edge1 e
        set e.carry = pro.carry
        from dataset.pr_outdegree pro
        where e.src= pro.node
        """

        # create table that group by dst, sum carry
        q7 = """
        create or replace table dataset.carrysum as 
        select dst as node, sum(carry) as sum
        from dataset.edge1
        group by dst
        """
        
        # update pr_outdegree: pr, carry
        q8 = """
        update dataset.pr_outdegree pro
        set pro.pr = cs.sum, pro.carry = cs.sum/pro.outdegree
        from dataset.carrysum cs
        where pro.node= cs.node
        """
        #
        job = client.query(q6)
        results = job.result()
        job = client.query(q7)
        results = job.result()
        job = client.query(q8)
        results = job.result()
    q9 = """
    select node as twitter_username, pr as page_rank_score
    from dataset.pr_outdegree 
    order by pr desc
    limit 100
    """
    job = client.query(q9)
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
        #print(results)


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

    funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    #funcs_to_test = [testquery]
    for func in funcs_to_test:
        rows = func(client)
        print ("\n====%s====" % func.__name__)
        print(rows)

    #bfs(client, 'A', 5)

if __name__ == "__main__":
  main()
