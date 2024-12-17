import pandas as pd
import neo4j
from neo4j import GraphDatabase

def node_creation_props(node_type, row):
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
    n = len(df.index)
    for i, row in df.iterrows():
        print(f'Writing node {i + 1} / {n}.')
        props = node_creation_props(node_type, row)
        statement = f'CREATE (n:{node_type} $props) RETURN n'
        
        with driver.session(database = database_name) as session:
            result = session.run(statement, parameters = props).single()
        
    return

def create_relationships_by_property(driver, relationship_type, node_from, node_to,
                                     on, database_name = 'neo4j'):
    statement = f'MATCH (a:{node_from}), (b:{node_to}) '
    statement += f'WHERE (a.{on}) IS NOT NULL AND (b.{on}) IS NOT NULL AND a.{on} = b.{on} '
    statement += f'CREATE (a)-[:{relationship_type}]->(b);'
    
    print('Executing\n', statement)
    
    with driver.session(database = database_name) as session:
        result = session.run(statement).single()
    
    return

def create_relationships_by_id(driver, relationship_type, id_, relation_dict, database_name = 'neo4j'):
    start_type = relation_dict['start_node']['type']
    start_field = relation_dict['start_node']['field']
    start_key = id_
        
    n = len(relation_dict['end_nodes'])
    for i, end_node in enumerate(relation_dict['end_nodes']):
        print(f'Creating relationship {i + 1} / {n} for node {id_}:{start_type}.')
        end_type = end_node['type']
        end_field = end_node['field']
        end_key = end_node['key']

        statement = f'MATCH (a:{start_type} {{{start_field}:{start_key}}}), '
        statement += f'(b:{end_type} {{{end_field}:{end_key}}}) '

        if 'props' in end_node.keys():
            props = {'props': end_node['props']}
            statement += f'CREATE (a)-[:{relationship_type} $props]->(b);'
        else:
            props = {}
            statement += f'CREATE (a)-[:{relationship_type}]->(b);'

        with driver.session(database = database_name) as session:
            result = session.run(statement, parameters = props).single()
    return

def create_index_by_node(driver, index_name, node_type, on, database_name = 'neo4j'):
    statement = f'CREATE INDEX {index_name} FOR (n:{node_type}) on (n.{on})'
    
    print('Executing\n', statement)

    with driver.session(database = database_name) as session:
        result = session.run(statement).single()
        
    return

def create_index_by_relation(driver, index_name, relation_type, on, database_name = 'neo4j'):
    statement = f'CREATE INDEX {index_name} FOR ()-[r:{relation_type}]-() ON (r.{on})'
    
    print('Executing\n', statement)
    
    with driver.session(database = database_name) as session:
        result = session.run(statement).single()
        
    return