CALL gds.graph.create.cypher(
    'Padrons',
    'MATCH (n) WHERE n:Persona OR n:Habitatge RETURN id(n) as id, labels(n) as labels',
    'MATCH (n)-[e]-(m) RETURN id(n) AS source, e.weight AS weight, id(m) AS target'
);

call gds.wcc.stream( 'Padrons')
yield componentId, nodeId
with componentId, collect(nodeId) as nodes,
size(collect(nodeId)) as mida
order by mida DESC 
match(n)
where id(n) in nodes AND mida =NUMBER
return n;

call gds.wcc.stream( 'Padrons')
yield componentId, nodeId
with componentId, collect(nodeId) as nodes,
size(collect(nodeId)) as mida
order by mida DESC 
match(m:Habitatge)
where id(m) in nodes
RETURN count(m) as numHab,mida,componentId;

call gds.wcc.stream( 'Padrons')
yield componentId, nodeId
with componentId, collect(nodeId) as nodes,
size(collect(nodeId)) as mida
order by mida DESC 
match(m:Persona)
where id(m) in nodes AND NOT (m)-[:VIU]->() 
RETURN count(componentId) as numcomponents;

