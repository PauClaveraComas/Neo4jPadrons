MATCH (p:Persona)-[:VIU]->(h:Habitatge{Municipi:'CR',Any_Padro:1866})
WHERE NOT p.Name ='nan' AND p.Name IS NOT NULL
RETURN COUNT(p) AS Numero_De_Persones,collect(DISTINCT p.Name) as Noms;

MATCH (h:Habitatge{Municipi:'SFLL'})
WHERE h.Any_Padro < 1840
RETURN h.Municipi,h.Any_Padro,collect(h.Id_Llar)
ORDER BY h.Any_Padro;

MATCH (p:Persona)-[:VIU]->(:Habitatge{Any_Padro:1838,Municipi:'SFLL'})<-[:VIU]-(:Persona{Name:'rafel',Surname:'marti'})
RETURN collect(p.Name);

MATCH (p:Persona)-[:VIU]->(:Habitatge{Any_Padro:1838,Municipi:'SFLL'})<-[:VIU]-(:Persona{Name:'rafel',Surname:'marti'})
RETURN p;

MATCH (p:Persona)-[:SAME_AS]-(p2:Persona)
WHERE toLower(p2.Name) = 'miguel' and toLower(p2.Surname) = 'ballester'
RETURN p,p2;

MATCH (p1:Persona)-[r:Familia]-(p2:Persona{Name:'antonio',Surname:'farran'})
WHERE p1 <> p2
RETURN p1.Name,p1.Surname,p1.Second_Surname,r.familiar;

MATCH (:Persona)-[f:Familia]-(:Persona)
RETURN COLLECT(DISTINCT f.familiar);

MATCH (h1:Habitatge{Municipi:'SFLL'})
WHERE h1.Carrer IS NOT NULL AND h1.Numero IS NOT NULL
RETURN (h1.Carrer)as Carrer,h1.Numero as Numero, COUNT(h1) AS NumeroSimilars, collect(h1.Any_Padro) as Llista_anys,collect(h1.Id_Llar) as Llista_ID
ORDER BY NumeroSimilars DESC
LIMIT 10;

MATCH (p2:Persona)-[f:Familia]->(p1:Persona)-[:VIU]->(h:Habitatge{Municipi:'CR'})
WHERE f.familiar = 'fill' OR f.familiar = 'filla'
WITH COUNT(f) as num_fills,p2
WHERE num_fills >3
RETURN num_fills,p2.Name,p2.Surname,p2.Second_Surname
ORDER BY (num_fills) DESC
LIMIT 20;

MATCH (p2:Persona)-[f:Familia]->(p1:Persona)-[:VIU]->(h:Habitatge{Municipi:'SFLL',Any_Padro:1881})
WHERE f.familiar = 'fill' or f.familiar = 'filla'
WITH COUNT(f) as num_fills,p2
MATCH (h:Habitatge{Municipi:'SFLL',Any_Padro:1881})
RETURN sum(num_fills) as NumFills,count(h) as NumHabitatges, avg(num_fills) as MitjaFills;

MATCH (p:Persona)-[:VIU]->(h:Habitatge{Municipi:'SFLL'})
WITH collect(DISTINCT p.Year)as llista_anys
UNWIND llista_anys as Year
MATCH (p:Persona)-[:VIU]->(h:Habitatge{Municipi:'SFLL',Any_Padro:Year})
WITH count(p) as numHab, h.Carrer as NomCarrer,Year
ORDER BY numHab
WITH collect(numHab+','+NomCarrer) as llista,Year
WITH split(llista[0],',') as nom,Year
RETURN nom[1] as Carrer,Year
ORDER BY Year;

