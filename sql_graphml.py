from py2neo import neo4j as neo
import sqlite3 as sql


NGRAM = "/var/www/ngram/ngrams.db"
CypherString = """CYPHER 2.0 
    merge (n:word {form: "%", freq: "%"})
    merge(m:word {form: "%", freq: "%"}) 
    create unique (n)-[:bigram {freq: "%"}]->(m) """


# define strings for GraphML and XML coding

XML_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
"""

GRAPHML_START = """<graphml xmlns="http://graphml.graphdrawing.org/xmlns"  
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
    http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
    """
GRAPHML_END = """</graphml>"""

GRAPH_START = """<graph id="nb.no/bigrams" edgedefault="directed">"""					
GRAPH_END = """</graph>"""

GRAPH_KEYS = """
    <key id = "form0" for = "node" attr.name = "form" attr.type = "string"/>
    <key id = "freq0" for = "all" attr.name = "frequency" attr.type = "int"/>
    <key id = "mi0" for = "edge" attr.name = "mi" attr.type = "float"/>
    <key id = "label0" for = "edge" attr.name = "label" attr.type = "string">
       <default>bigram</default>
    </key>"""

#GRAPH_EDGE and GRAPH_NODE take parameters as format strings

# The node takes parameters {nodeid} (the row number from sql) and {form} (the spelling)
GRAPH_NODE ="""    <node id="{nodeid}">
        <data key = "form0">{form}</data>
        <data key = "freq0">{frequency}</data>
    </node>
"""
#Edges take parameters {from} and {end} (the id of the matching node), and the {frequency} and {mi}
GRAPH_EDGE =""" <edge  source = "{source}" target = "{target}">
        <data key = "freq0">{frequency}</data>
        <data key = "mi0">{mi}</data>
    </edge>
"""


def set_cursor(DB):
    try:
        connection = sql.connect(DB)
        cursor = connection.cursor()
        return(cursor, connection)
    except:
        print( "Noe gikk galt med " + DB)
   
def close_connection(connection):
    connection.close()

def validate(string):
    string = string.replace("<","&lt;")
    string = string.replace("&", "&amp;")
    return(string)


def or_clause( column, words):
    expr = ""
    if words != []:
        expr = """ ({col} = '{word}' """.format(col=column, word=words[0])
        for w in words[1:]:
            expr += """ or {col} = '{word}'""".format(col=column, word=w)
        expr += ")"
    return(expr)
    
def import_sql(sql_cursor, graph_xml_filename, words=[]):
    
    graphXML = open(graph_xml_filename, "w")
    
    #output the header and start tags
    graphXML.write(XML_HEADER)
    
    graphXML.write(GRAPHML_START)
    graphXML.write(GRAPH_KEYS)
    
    #Write out the sequence of nodes and relations
    graphXML.write(GRAPH_START)


    #create a list of conditions
    first_column = or_clause("a.first", words)
    second_column = or_clause("a.second", words)
    where1 = ""
    where2 = ""
    if first_column != "":
        where1 = "where " + first_column 
        if second_column != "":
            where2 = where1 + " and " + second_column 


    #prepare sql statements
    sql_str_unigram =  """select a.* from unigram_rows as a {conditions}""".format(conditions = where1)
 
    if where2 != "":
        sql_str_bigram = """select b.rowid as source, c.rowid as target, a.freq, a.mi 
        from bigram_mi as a, unigram as b, unigram  as c 
        {conditions} and a.first = b.first and a.second=c.first""".format(conditions = where2)
    else:
        sql_str_bigram =  """select * from bigram_rows"""   
 
    for (row_id, first, freq) in sql_cursor.execute(sql_str_unigram):
        #print(freq, first, firstfreq, second, secondfreq)
        first_validated = validate(first)
        node_xml = GRAPH_NODE.format(frequency = freq, nodeid = row_id, form=first_validated)
        graphXML.write(node_xml)
    #then create the relations
  

    for (start, end, freq, mi) in sql_cursor.execute(sql_str_bigram):
        edge_xml = GRAPH_EDGE.format(source = start, target = end, frequency = freq, mi = mi)
        graphXML.write(edge_xml)
        
    #close the graph
    graphXML.write(GRAPH_END)
    graphXML.write(GRAPHML_END)