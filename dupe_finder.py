#! /usr/bin/python

import neo4j.v1 as neo4j
import configparser
from logging import debug, info, warning, error
import os
from collections import defaultdict
DEFAULT_CONFIG = configparser.ConfigParser().read_dict(
    {'database':{'username':'neo4j', 'password':'neo4j', 'host':'localhost','port':7687},
     'paths':{'path1':'C:\\'}})


def load_config(config_file='./dupe_finder.conf'):
    """
    Read configuration from file, formatted according to ConfigParser (traditional Windows .ini and Unix .conf).
    Returns the default configuration above if no such file is found.

    A sample config file is as follows:
    ::
        [database]
          username = neo4j
          password = neo4j
          host = localhost
          port = 7687

        [paths]
          path1 = C:/
          path2 = D:/
          path3 = E:/
    ::

    :param config_file: Sets the file to read from. Defaults to ./dupe_finder.conf
    :return: The parsed configuration.
    """
    config = configparser.ConfigParser()
    config = config.read(config_file)
    if len(config) != 0:
        return config
    warning('Config not found at {}. Using default settings.'.format(config_file))
    return DEFAULT_CONFIG


def get_database_connection(config):
    """
    Create the Neo4j database connection defined in the config.
    :param config:
    :return:
    """
    db_config = config['database']
    n4j = neo4j.GraphDatabase.driver('bolt://{host}:{port}'.format(host=db_config['host'], port=db_config['port']),
                                     auth=(db_config['username'], db_config['password']))
    return n4j

def split_paths_by_device(config):
    """
    Splits the paths in the config according to their device. Should work well on Windows where the file system is
    generally by partition, but on Linux it might be preferable to check this at more levels, or at least provide an option.

    :param config: ConfigParser object including path list.
    :return:
    """
    device_hierarchy = defaultdict(list)
    for each_path in config['paths'].values():
        device_hierarchy[os.stat(each_path).st_dev].append(each_path)
    return device_hierarchy

def enumerate_and_store_path(path, db_conn: neo4j.Driver, parent_node=None):
    """

    :param path:
    :param db_conn:
    :return:
    """
    with db_conn.session() as session:
        existing = (session.run('MATCH (n:Path) WHERE n.path="{path}" RETURN n'.format(path)).single())

        if existing is None:
            existing = session.run('CREATE (n:Path {path:"{path}", finished="False") RETURN n')
            if parent_node is not None:
                session.run('MATCH (m:Path) WHERE ID(m)={parent_node}'+
                            'MATCH (n:Path) WHERE ID(n)={existing}'
                            'CREATE (m)-[:CONTAINS]->(n)'.format(parent_node=parent_node.id, existing=existing.id))

        if existing is not None and existing.properties['finished'] == "True":
            #we're already done here - skip this path
            return

        with os.scandir(path) as scan:
            for each_direntry in scan:
                new = session.run('CREATE')










