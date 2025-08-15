-Checar EVAL IV
SELECT EVENTCODE, EPDATE,FIRSTONSALE,CIENOCIEI, SERIESEVENT, ARTIST, VENUE, EVZONE, PROMOTER_ID,PROMOTER_NAME,  P_CATEGORY
FROM tble1
--FROM table2
--FROM table3
WHERE EVENTCODE IN (
		'EPD0620'
)
AND EPDATE > '2025-06-01';

--CAMBIOS AL EVAL IV
--Cambiar CIENOCIE: Insertar en tabla para que aparezca automáticamente el valor "EVENTO CIE" en EVAL IV
--PAra convertir el epdate en string con 10
--SELECT EVENTCODE,LEFT(CAST( EPDATE AS STRING),10), ARTIST,CIENOCIEI
--FROM table
--WHERE ARTIST LIKE 'Blink-182'
--AND CIENOCIEI = 'TERCEROS';

/*INSERT INTO table
(Fecha_de_evento, Clave_de_evento)
VALUES 
('2026-01-31','EME0131N'),
--('2024-08-07','EMET0807'),
--('2024-09-19','EMET0919'),
--('2024-06-01','EMET0601'),
--('2024-10-12','EAPC1012'),
--('2024-06-04','ELU0604'),
--('2024-08-25','EET0825'),
--('2024-08-28','EEBN828N'),
--('2024-05-22','EPCW0522');*/

--Cambiar/Homologar Nombre de Artista (EL cambio se hace en dos lados Eval y EVAL_DBKS)

--UPDATE table
--SET ARTIST = 'Yuri & Cristian'
--SET PROMOTER_ID  = ''
--SET PROMOTER_NAME =''
--WHERE EVENTCODE IN (
--  'EAN0618',
--	'EAN0619'
--)
--AND EPDATE BETWEEN '2024-05-01' AND '2024-12-31';

--UPDATE table
--SET ARTIST = 'Yuri & Cristian'
--SET PROMOTER_ID  = ''
--SET PROMOTER_NAME =''
--WHERE EVENTCODE IN (
--  'EAN0618',
--	'EAN0619'
--)
--AND EPDATE BETWEEN '2024-05-01' AND '2024-12-31';

--Checar MOP
SELECT CLAVE, EPDATE, EVENTO_CONCEPTO, SUM(NTQTY) AS BOLETOS, SUM (NDOLLARS) AS VENTAS, SUM (SCHGDOL) AS CXS
FROM table 
WHERE SDATE > '2024-02-01'
AND CLAVE IN (
	'EPD1010N',	
	'EAB0531',
	'EAB0628',
	'EAN0623T',
	'ET80518'
)
GROUP BY CLAVE,EPDATE, EVENTO_CONCEPTO
ORDER BY CXS DESC;

--UPDATE MOP

--UPDATE table 
--SET EVENTONAME = 'The national x the war of drugs'
--SET EPDATE = CAST('2024-06-21' AS TIMESTAMP)
--WHERE EVENTKEY IN (
	--'EPD1010N')
--AND EPDATE > '2024-03-01';

--CHECK REPCIE_LAST
SELECT * FROM table
WHERE EventKey IN( 	
	'EPD1010N',	
	'EAB0531',
	'EAB0628',
	'EAN0623T',
	'ET80518')
AND EPDATE > '2024-01-01'
order by epdate asc;

--CHECK REPSRCIE
SELECT * FROM table
WHERE EventKey IN (	'EPD1010N',	
	'EAB0531',
	'EAB0628',
	'EAN0623T',
	'ET80518'
	)
AND EPDATE > '2024-01-01'
--GROUP BY Epdate
order by epdate asc;

--UPDATE REPSRCIE
--UPDATE table
--SET EVENTONAME = 'The national x the war of drugs'
--SET EPDATE = CAST('2024-06-21' AS TIMESTAMP)
--WHERE EVENTKEY IN (
	--'EPD1010N')
--AND EPDATE > '2024-03-01';


--EventZone
SELECT evZone,VENUE
FROM table
WHERE EPDATE >'2024-01-01'
group by evZone, VENUE;


--INSERT TOTAL
--INSERT INTO table`(idRow,
--Estatus,Epdate, EventoName,VenueNum,VenueName,evZone,ZoneAbrev,Lugar,EventKey,Promotor,DiasVenta,DiasPendientes,NsAcumulado,VentasXDia,NdAcumulado,PrecioPromedio,Comp,UnPeso,TotalCortesias,TotalNs,PorcOcupacion,TotalNd,Opens,Hold,CapacidadTotal,NsPromedio,NsMinus3,NsMinus2,NsMinus1,VarNs,NdPromedio,NdMinus3,NdMinus2,NdMinus1,VarNd,AvDiaSO,AudRepDate,IsShow,IsEvtCIE,NumFunciones,ShowVenue,IsTemporada,TemporadaName,IsAbono,FunctionsAbono,IsFestival,IsProvincia,IsCancel,Boletera,IdCategoria,CategoriaName,IsDurabilidad,IsTempoAsociada,Ponderacion,Indice,IsBreakDownTempo)
--VALUES (618,'RUNNING','2024-04-06','MITSKI',3,'TEATRO METROPÓLITAN',1,'','CIUDAD DE MÉXICO','EME0406','OCESA',3,181,2934,733,3332740,1135.9,0,0,0,2934,0,4201870372.3,0,190,3124,0,290,0,0,0,0,328730,0,0,0,1,'2023-10-08',0,1,1,1,0,'',0,0,0,0,0,1,0,'',0,0,0,0,0);

--INSERT EMERGENCIA 
--INSERT INTO table (`Epdate`, `EventoName`, `VenueName`, `evZone`, `ZoneAbrev`, `Lugar`, `EventKey`, `Opens`, `PrecioPromedio`, `AudRepDate`) VALUES
--('2024-06-06','RINGO STARR','AUDITORIO NACIONAL',1,'MX','CDMX','EAN0606',3252,1426,'2023-11-21');

--UPDATE table
--SET EPDATE= '2024-06-06'
--WHERE EventKey IN ('EAN0606')
--AND EVENTONAME = 'RINGO STARR';

----------------------------------------------------------------

SELECT date(date) as date,
  customDimensions.value AS ID_Promotor,
  COUNT(DISTINCT clientId) AS Usuarios
FROM 
 table,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product,
  UNNEST(hits.customDimensions) AS customDimensions
WHERE customDimensions.index = 47
GROUP BY date, customDimensions.value
ORDER BY date ASC, customDimensions.value ASC

-----------------------------------------------------------------

SELECT *
FROM table
WHERE _TABLE_SUFFIX BETWEEN '20200101' AND '20240430';
