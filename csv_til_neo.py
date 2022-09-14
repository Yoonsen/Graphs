from py2neo import neo4j as neo
import sqlite3 as sql


NGRAM = "/var/www/ngram/ngrams.db"
CypherString = """CYPHER 2.0 
    merge (n:word {form: "%", freq: "%"})
    merge(m:word {form: "%", freq: "%"}) 
    create unique (n)-[:bigram {freq: "%"}]->(m) """)


def set_cursor(DB):
    try:
        connection = sql.connect(DB)
        cursor = connection.cursor()
        return(cursor, connection)
    except:
        print( "Noe gikk galt med " + DB)
   
def close_connection(connection):
    connection.close()
    
def import_csv(sql_cursor, neo4j_db):
    
    sql_str =  """select * from bigram_with_number"""
    query = neo.CypherQuery(neo4j_db, """CYPHER 2.0 
                merge (n:word {form: {firstvar}, freq: {freqfirst}})
                merge(m:word {form: {secondvar}, freq: {freqsecond}}) 
                create unique (n)-[:bigram {freq: {freqbigram}}]->(m) """)
    for (freq, first, firstfreq, second, secondfreq) in sql_cursor.execute(sql_str):
        #print(freq, first, firstfreq, second, secondfreq)
        result = query.execute(freqbigram = freq, firstvar = first, freqfirst=firstfreq, secondvar=second, freqsecond=secondfreq)

def import_sql(sql_cursor, filename):
    
    neoFile = open(filename, "w")
    sql_str =  """select * from bigram_with_number"""
    CypherString = """CYPHER 2.0 
                merge (n:word {form: "%", freq: "%"})
                merge(m:word {form: "%", freq: "%"}) 
                create unique (n)-[:bigram {freq: "%"}]->(m) """)
    for (freq, first, firstfreq, second, secondfreq) in sql_cursor.execute(sql_str):
        #print(freq, first, firstfreq, second, secondfreq)
        result = query.execute(freqbigram = freq, firstvar = first, freqfirst=firstfreq, secondvar=second, freqsecond=secondfreq)