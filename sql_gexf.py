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

GEXF_START = """<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
    """
GEXF_END = """</gexf>"""

GRAPH_START = """<graph mode="static" defaultedgetype="directed">"""					
GRAPH_END = """</graph>"""

GEXF_ATTRIBUTES = """
    <attributes class="node" mode="static">
        <attribute id="freq0" title="n.frequency" type="int"/>
        <attribute id="form0" title="form" type="string"/>
    </attributes>
    <attributes class="edge" mode="static">
        <attribute id = "freq1" title = "e.frequency" type = "int"/>
        <attribute id="mi0" title="MI" type = "float"/>
    </attributes>
   """

#GRAPH_EDGE and GRAPH_NODE take parameters as format strings

# The node takes parameters {nodeid} (the row number from sql) and {form} (the spelling)
NODES_START = """
    <nodes>
"""
NODES_END = """     </nodes>
"""

# The nodes are made with a parametrized string, in which the values are
# supplied by the sql-query
GRAPH_NODE ="""    
    <node id="{nodeid}" label="{form}">
    <attvalues>
        <attvalue for = "form0" value = "{form}"/>
        <attvalue for = "freq0" value = "{frequency}"/>
    </attvalues>
    </node>
"""

EDGES_START = """
    <edges>
"""
EDGES_END = """
    </edges>
"""


#Edges also take parameters: {from} and {end} (the id of the matching node), and the {frequency} and {mi}
GRAPH_EDGE =""" 
    <edge  source = "{source}" target = "{target}" weight = "{mi}">
        <attvalues>
            <attvalue for = "freq1" value = "{frequency}"/>
            <attvalue for = "mi0" value = "{mi}"/>
        </attvalues>
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
    string = string.replace ('"', "&quot;")
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
    
    graphXML.write(GEXF_START)
    graphXML.write(GEXF_ATTRIBUTES)
    
    #Write out the sequence of nodes and relations
    graphXML.write(GRAPH_START)


    #create a list of conditions
    first_column = or_clause("a.first", words)
    second_column = or_clause("a.second", words)
    where1 = ""
    where2 = ""
    if first_column != "":
        where1 = "and " + first_column
        if second_column != "":
            where2 = where1 + " and " + second_column 


    #prepare sql statements
    sql_str_unigram =  """select a.* from unigram_rows as a where freq > 10 {and_conditions} """.format(and_conditions = where1)
 
    if where2 != "":
        sql_str_bigram = """select b.rowid as source, c.rowid as target, a.freq, a.mi 
        from bigram_mi as a, unigram as b, unigram  as c where mi > 2
        {and_conditions} and a.first = b.first and a.second=c.first """.format(and_conditions = where2)
    else:
        sql_str_bigram =  """select * from bigram_rows where mi > 2"""   
    
    #write nodes section
    graphXML.write(NODES_START)
    
    for (row_id, first, freq) in sql_cursor.execute(sql_str_unigram):
        first_validated = validate(first)
        node_xml = GRAPH_NODE.format(frequency = freq, nodeid = row_id, form=first_validated)
        graphXML.write(node_xml)
    graphXML.write(NODES_END)    
        
        
        
    #then create the relations
  
    graphXML.write(EDGES_START)
    for (start, end, freq, mi) in sql_cursor.execute(sql_str_bigram):
        edge_xml = GRAPH_EDGE.format(source = start, target = end, frequency = freq, mi = mi)
        graphXML.write(edge_xml)
    graphXML.write(EDGES_END)
    
    
    #close the graph
    graphXML.write(GRAPH_END)
    graphXML.write(GEXF_END)

def sql_to_graph(sql_cursor, graph_xml_filename, words):
    
    graphXML = open(graph_xml_filename, "w")
    
    #output the header and start tags
    graphXML.write(XML_HEADER)
    
    graphXML.write(GEXF_START)
    graphXML.write(GEXF_ATTRIBUTES)
    
    #Write out the sequence of nodes and relations
    graphXML.write(GRAPH_START)


    #create a list of conditions
  
    
  
  
    graphXML.write(NODES_START)
    
    for (row_id, first, freq) in sql_cursor.execute(sql_str_unigram):
        first_validated = validate(first)
        node_xml = GRAPH_NODE.format(frequency = freq, nodeid = row_id, form=first_validated)
        graphXML.write(node_xml)
    graphXML.write(NODES_END)    
        
        
        
    #then create the relations
  
    graphXML.write(EDGES_START)
    for (start, end, freq, mi) in sql_cursor.execute(sql_str_bigram):
        edge_xml = GRAPH_EDGE.format(source = start, target = end, frequency = freq, mi = mi)
        graphXML.write(edge_xml)
    graphXML.write(EDGES_END)
    
    
    #close the graph
    graphXML.write(GRAPH_END)
    graphXML.write(GEXF_END)