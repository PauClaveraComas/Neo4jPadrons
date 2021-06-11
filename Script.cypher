CREATE CONSTRAINT UnicaLlar ON (h:Habitatge) ASSERT (h.Id_Llar,h.Municipi,h.Any_Padro) IS NODE KEY;
CREATE CONSTRAINT UnicaPersona ON (p:Persona) ASSERT p.Id IS UNIQUE;

LOAD CSV FROM 'file:///Habitatges.csv' AS Habitatge
WITH toInteger(Habitatge[1]) AS Id_Llar, Habitatge[0] AS Municipi, toInteger(Habitatge[2]) AS Any_Padro, Habitatge[3] AS Carrer, toInteger(Habitatge[4]) AS Numero
WHERE Id_Llar IS NOT null AND Municipi is not null AND Any_Padro is not NULL AND Carrer IS NOT NULL AND Numero IS NOT NULL
MERGE (h:Habitatge {Id_Llar: Id_Llar, Carrer:Carrer, Any_Padro:Any_Padro, Municipi:Municipi, Numero:Numero})
    SET h.Id_Llar = Id_Llar, h.Carrer = Carrer, h.Any_Padro = Any_Padro, h.Municipi = Municipi, h.Numero = Numero;


LOAD CSV FROM 'file:///Individual.csv' AS Persona
WITH toInteger(Persona[0]) AS Id, toInteger (Persona[1]) AS Year, Persona[2] AS Name, Persona[3] AS Surname, Persona[4] AS Second_Surname
WHERE Id IS NOT null 
MERGE (p:Persona{Id:Id,Year:Year,Name:Name,Surname:Surname,Second_Surname:Second_Surname})
    SET p.Id = Id, p.Year = Year, p.Name = Name, p.Surname = Surname, p.Second_Surname = Second_Surname;


LOAD CSV FROM 'file:///FAMILIA.csv' AS Familia
WITH toInteger(Familia[0]) AS ID_1, Familia[1] AS Relacio, Familia[2] AS Relacio_Harmonitzada, toInteger(Familia[3]) AS ID_2
WHERE ID_1 IS NOT NULL AND ID_2 IS NOT NULL
MERGE (f1:Persona{Id:ID_1})
MERGE (f2:Persona{Id:ID_2})
CREATE (f1)-[:Familia {familiar:Relacio_Harmonitzada}]->(f2);

LOAD CSV FROM 'file:///VIU.csv' AS VIU
WITH toInteger(VIU[0]) AS IND, VIU[2] AS Location, toInteger(VIU[3]) AS Year, toInteger(VIU[4]) AS HOUSE_ID
WHERE IND IS NOT NULL AND Location IS NOT NULL AND Year IS NOT NULL AND HOUSE_ID IS NOT NULL
MERGE (hab:Habitatge{Municipi:Location,Id_Llar:HOUSE_ID,Any_Padro:Year})
MERGE (per:Persona{Id:IND})
CREATE (per)-[:VIU]->(hab);

LOAD CSV FROM 'file:///same_as.csv' AS SAME_AS
WITH toInteger(SAME_AS[0]) AS ID1, toInteger(SAME_AS[2]) AS ID2
WHERE ID1 IS NOT NULL AND ID2 IS NOT NULL
MERGE (p1:Persona{Id:ID1})
MERGE (p2:Persona{Id:ID2})
CREATE (p1)-[:SAME_AS]->(p2)
RETURN *;

