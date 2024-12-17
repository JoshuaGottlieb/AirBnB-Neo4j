import pandas as pd
import neo4j
from neo4j import GraphDatabase

def node_creation_props(node_type, row):
    '''
    Creates a dictionary of properties to use for node creation while iterating through a Pandas DataFrame.
    
    args:
        node_type: 'Listing', 'Host', or 'Guest', to use for determining which properties to extract
        row: Pandas DataFrame row, containing information to extract
        
    returns dict of node properties
    '''
    if node_type == 'Listing':
        props = {
            'props': {
                'listing_id': row['id'],
                'listing_url': row['listing_url'],
                'description': row['description'],
                'host_id': row['host_id'],
                'neighborhood': row['neighbourhood_cleansed'],
                'borough': row['neighbourhood_group_cleansed'],
                'property_type': row['property_type'],
                'room_type': row['room_type'],
                'accommodates': row['accommodates'],
                'bathrooms': row['bathrooms'],
                'bedrooms': row['bedrooms'],
                'beds': row['beds'],
                'amenities': row['amenities'],
                'price': row['price'],
                'min_nights': row['minimum_nights'],
                'max_nights': row['maximum_nights'],
                'available': row['has_availability'],
                'review_count': row['number_of_reviews'],
                'review_rating': row['review_scores_rating']
            }
        }
    elif node_type == 'Host':
        props = {
            'props': {
                'host_id': row['host_id'],
                'host_name': row['host_name'],
                'joined': row['host_since'],
                'superhost': row['host_is_superhost'],
                'total_listings': row['host_total_listings_count'],
                'contact_methods': row['host_verifications']
            }
        }
    elif node_type == 'Guest':
        props = {
            'props': {
                'guest_id': row['reviewer_id'],
                'guest_name': row['reviewer_name'],
            }
        }
    
    return props

def create_nodes(driver, node_type, df, database_name = 'neo4j'):
    '''
    Creates nodes in a Neo4j database from a Pandas DataFrame.
    
    args:
        driver: Neo4j GraphDatabase.driver() object to use for connecting to the Neo4j database
        node_type: 'Listing', 'Host', or 'Guest', to use for determining which properties to extract
        df: Pandas DataFrame containing data to use for populating nodes
        database_name: str, name to pass to driver.session() for transactions
    '''
    n = len(df.index)
    for i, row in df.iterrows():
        print(f'Writing node {i + 1} / {n}.')
        # Create property dictionary
        props = node_creation_props(node_type, row)
        
        # Create Cypher statement and execute
        statement = f'CREATE (n:{node_type} $props) RETURN n'
        
        with driver.session(database = database_name) as session:
            session.run(statement, parameters = props)
        
    return

def create_relationships_by_property(driver, relationship_type, node_from, node_to,
                                     on, database_name = 'neo4j'):
    '''
    Creates relationships between nodes in a Neo4j database based on a shared node property.
    
    args:
        driver: Neo4j GraphDatabase.driver() object to use for connecting to the Neo4j database
        relationship_type: str, label to use for relationship
        node_from: str, starting node type
        node_to: str, ending node type
        on: str, property to use as a shared key for matching nodes
        database_name: str, name to pass to driver.session() for transactions
    '''
    # Construct Cypher statement
    statement = f'MATCH (a:{node_from}), (b:{node_to}) '
    statement += f'WHERE (a.{on}) IS NOT NULL AND (b.{on}) IS NOT NULL AND a.{on} = b.{on} '
    statement += f'CREATE (a)-[:{relationship_type}]->(b);'
    
    print('Executing\n', statement)
    
    with driver.session(database = database_name) as session:
        session.run(statement)
    
    return

def create_relationships_by_id(driver, relationship_type, id_, relation_dict, database_name = 'neo4j'):
    '''
    Creates relationships between nodes in a Neo4j database based on an explicit mapping from starting node id.
    
    args:
        driver: Neo4j GraphDatabase.driver() object to use for connecting to the Neo4j database
        relationship_type: str, label to use for relationship
        id_: int, starting node id
        relation_dict: dict with two keys, 'start_node' and 'end_nodes'. The value at 'start_node' should be
            a dict with two keys 'type' and 'field', while the value at 'end_nodes' should be a list of dicts
            with three keys 'type', 'field', and 'key' and optionally 'props'.
            Used for explicitly mapping start and end nodes to create the relationship.
        database_name: str, name to pass to driver.session() for transactions
    '''
    # Extract start_node information
    start_type = relation_dict['start_node']['type']
    start_field = relation_dict['start_node']['field']
    start_key = id_
        
    n = len(relation_dict['end_nodes'])
    for i, end_node in enumerate(relation_dict['end_nodes']):
        print(f'Creating relationship {i + 1} / {n} for node {id_}:{start_type}.')
        # Extract end_node information
        end_type = end_node['type']
        end_field = end_node['field']
        end_key = end_node['key']

        # Construct Cypher statement
        statement = f'MATCH (a:{start_type} {{{start_field}:{start_key}}}), '
        statement += f'(b:{end_type} {{{end_field}:{end_key}}}) '

        # If relation properties are included, add to the Cypher statement
        if 'props' in end_node.keys():
            props = {'props': end_node['props']}
            statement += f'CREATE (a)-[:{relationship_type} $props]->(b);'
        else:
            props = {}
            statement += f'CREATE (a)-[:{relationship_type}]->(b);'

        with driver.session(database = database_name) as session:
            session.run(statement, parameters = props)
    return

def create_index_by_node(driver, index_name, node_type, on, database_name = 'neo4j'):
    '''
    Creates an index in Neo4j on a node property.
    
    args:
        driver: Neo4j GraphDatabase.driver() object to use for connecting to the Neo4j database
        index_name: str, name for the index
        node_type: str, node type to match
        on: str, property to create the index on
        database_name: str, name to pass to driver.session() for transactions
    '''
    # Construct Cypher statement
    statement = f'CREATE INDEX {index_name} FOR (n:{node_type}) on (n.{on})'
    
    print('Executing\n', statement)

    with driver.session(database = database_name) as session:
        session.run(statement)
        
    return

def create_index_by_relation(driver, index_name, relation_type, on, database_name = 'neo4j'):
    '''
    Creates an index in Neo4j on a relationship property.
    
    args:
        driver: Neo4j GraphDatabase.driver() object to use for connecting to the Neo4j database
        index_name: str, name for the index
        relation_type: str, relation type to match
        on: str, property to create the index on
        database_name: str, name to pass to driver.session() for transactions
    '''
    # Construct Cypher statement
    statement = f'CREATE INDEX {index_name} FOR ()-[r:{relation_type}]-() ON (r.{on})'
    
    print('Executing\n', statement)
    
    with driver.session(database = database_name) as session:
        session.run(statement)
        
    return