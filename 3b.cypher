MATCH (h1:Habitatge),(h2:Habitatge)
WHERE h1.Carrer = h2.Carrer AND h1.Numero = h2.Numero AND h1.Municipi = h2.Municipi AND h1.Any_Padro > h2.Any_Padro
CREATE (h1)-[:MATEIX_HAB]->(h2)
RETURN h1,h2;

CALL gds.graph.create.cypher(
    'padrons3b',
    'MATCH (n) WHERE n:Persona OR n:Habitatge RETURN id(n) as id, labels(n) as labels',
    'MATCH (n)-[e]-(m) WHERE NOT e:SAME_AS RETURN id(n) AS source, e.weight AS weight, id(m) AS target'
);

CALL gds.nodeSimilarity.stream('padrons3b',{nodeLabels:[‘Habitatge’]) 

YIELD
  node1,
  node2,
  similarity;

CALL gds.nodeSimilarity.stream('padrons3b',{nodeLabels:['Persona']}) 

YIELD
  node1,
  node2,
  similarity;
