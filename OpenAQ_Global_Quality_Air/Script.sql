# Cria o schema
CREATE SCHEMA project_openaq;

# Cria a tabela
CREATE TABLE project_openaq.TB_GLOBAL_QUALITY_AIR (
	`location` text,
	`city` text,
	`country` text,
	`pollutant` text,
	`value` text,
	`timestamp` text,
	`unit` text,
	`source_name` text,
	`latitude` text,
	`longitude` text,
	`averaged_over_in_hours` text); 

# Carrega os dados via linha de comando
	#Conecta no MySQL via linha de comando no Windows OS -> mysql --local-infile=1 -u root -p
    #Concessão dos privilégios -> SET GLOBAL local_infile = true;
    #Carrega o dataset -> LOAD DATA LOCAL INFILE 'C:/Users/Lorrainy/OpenAQ_Global_Quality_Air/dataset.csv' INTO TABLE `project_openaq.TB_GLOBAL_QUALITY_AIR` CHARACTER SET UTF8 FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES;
    
# Limpeza e transformação dos dados

	#Visão geral dos dados
	SELECT * FROM project_openaq.TB_GLOBAL_QUALITY_AIR    
	LIMIT 0, 1000;
    
    #Quantidade de Registros
    SELECT count(*) FROM project_openaq.TB_GLOBAL_QUALITY_AIR; #4.000 registros
    
    #Tratamento de valores ausentes
    SELECT COUNT(*) FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(location, '') IS NULL; # 3 registros
    SELECT COUNT(*)  FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(city, '') IS NULL; # 34 registros
    SELECT COUNT(*)  FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(country, '') IS NULL; # 0 registros
    SELECT COUNT(*)  FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(pollutant, '') IS NULL; # 0 registros
    SELECT COUNT(*)  FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(value, '') IS NULL; # 0 registros 
    SELECT COUNT(*)  FROM project_openaq.TB_GLOBAL_QUALITY_AIR WHERE NULLIF(unit, '') IS NULL; # 0 registros 
    
    SET SQL_SAFE_UPDATES = 0;
    
    UPDATE project_openaq.TB_GLOBAL_QUALITY_AIR
    SET location = 'Other'
    WHERE location = '';
    
    UPDATE project_openaq.TB_GLOBAL_QUALITY_AIR
    SET city = 'Other'
    WHERE city = '';
    COMMIT;
    
    SET SQL_SAFE_UPDATES = 1;
    
	# Detecção e Remoção de Registros Duplicados
	SELECT location, city, country, timestamp, pollutant, value, COUNT(*)
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR
	GROUP BY location, city, country, timestamp, pollutant, value
	HAVING COUNT(*) > 1; # Não há registros duplicados
    
    # Convertento ppm para µg/m³
		# No conjunto há dois tipos de unidade de medida, portanto é necessário tornar os valores homogêneos. Em função dos valore para os poluentes pm10 e pm2 estarem apenas em µg/m³ 
        #e sua densidade molecular depender de sua composição que é variada, tornando, assim, impossível defini-la, optei por transformar os demais valores de ppm para µg/m³.
        # A fórmula para conversão de ppm (partes por milhão) para µg/m³ (microgramas por metro cúbico)  é: 0.0409 * valor em µg/m³/ mol (densidade molecular da matéria)
        
        SET SQL_SAFE_UPDATES = 0;
		UPDATE project_openaq.TB_GLOBAL_QUALITY_AIR
        SET value = CASE WHEN pollutant = 'bc'   THEN 24.45 * value / 12.011
						 WHEN pollutant = 'co'   THEN 24.45 * value / 28.01
                         WHEN pollutant = 'no2'  THEN 24.45 * value / 46.01
                         WHEN pollutant = 'o3'   THEN 24.45 * value / 48.00
                         WHEN pollutant = 'so2'  THEN 24.45 * value / 64.06
					ELSE value END
        WHERE unit = 'ppm'; COMMIT;
        SET SQL_SAFE_UPDATES = 1;

    # "Pivoteando" a tabela em função dos poluentes
    
    SELECT DISTINCT(pollutant) FROM project_openaq.TB_GLOBAL_QUALITY_AIR ORDER BY pollutant;
		#bc	  - Carbono negro,  seja, partículas que contêm carbono na sua constituição e absorvem radiação
        #co   - Monóxido de carbono
        #no2  - Dióxido de ozônio
        #o3   - Ozônio
        #pm10 - Partículas inaláveis em suspensão com diâmetro inferior a 10 micrómetros
        #pm25 - Partículas inaláveis em suspensão com diâmetro inferior a 2,5 micrómetros
        #so2  - Dióxido de enxofre
    
    CREATE TABLE project_openaq.TB_GLOBAL_QUALITY_AIR_ETL AS
		SELECT
				location,
				city,
				country,
				SUM(IF(pollutant = 'bc', value, 0)) AS bc,
				SUM(IF(pollutant = 'co', value, 0)) AS co,
				SUM(IF(pollutant = 'no2', value, 0)) AS no2,
				SUM(IF(pollutant = 'o3', value, 0)) AS o3,
				SUM(IF(pollutant = 'pm10', value, 0)) AS pm10,
				SUM(IF(pollutant = 'pm25', value, 0)) AS pm25,
				SUM(IF(pollutant = 'so2', value, 0)) AS so2,
				'µg/m³' AS unit,
				timestamp,
				source_name,
				latitude,
				longitude,
				averaged_over_in_hours
				
		FROM project_openaq.TB_GLOBAL_QUALITY_AIR
		GROUP BY 
				location,
				city,
				country,
				timestamp,
				source_name,
				latitude,
				longitude,
				averaged_over_in_hours;
    
#Análise dos dados
	# 1- Quais poluentes foram considerados na pesquisa?
		SELECT DISTINCT pollutant FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL;
        
	#2- Rank dos 5 paises com maior concentração de No2, Pm10 e Pm2,5 em 2020, ambos grupos de poluentes que se originam principalmente de 
	  # atividades humanas relacionadas à queima de combustíveis fósseis
	
    # NO2
    SELECT country AS pais,
			   ROUND(AVG(no2), 2) media_no2
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
	WHERE DATE_FORMAT(timestamp, '%Y') = '2020'
	GROUP BY country
	ORDER BY media_no2 DESC
	LIMIT 0, 5;
        #PL	10.25
		#GB	8.86
		#NO	7.43
		#IN	4.33
		#ES	3.73
    
    #PM10
    SELECT country AS pais,
			   ROUND(AVG(pm10), 2) media_no2
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
	WHERE DATE_FORMAT(timestamp, '%Y') = '2020'
	GROUP BY country
	ORDER BY media_no2 DESC
	LIMIT 0, 5;
		#NO	40.07
		#IN	34.81
		#PL	26.13
		#TH	20.89
		#MX	14.84
	
    #PM2,5
    SELECT country AS pais,
			   ROUND(AVG(pm25), 2) media_no2
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
	WHERE DATE_FORMAT(timestamp, '%Y') = '2020'
	GROUP BY country
	ORDER BY media_no2 DESC
	LIMIT 0, 5;
		#CN	97
		#NP	85
		#PK	69
		#IZ	50
		#UG	46
    
	# 2- Qual foi a média de poluição ao longo do tempo provocada pelo poluente para esses 5 países? (Média móvel)

			SELECT country AS pais,
				   DATE_FORMAT(timestamp, "%Y-%m") AS data_coleta,
                   ROUND(AVG(no2) OVER(PARTITION BY country ORDER BY DATE_FORMAT(timestamp, "%Y-%m")), 2) AS media_valor_n02
			FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
			WHERE country IN ('PL', 'GB', 'NO', 'IN', 'ES') AND DATE_FORMAT(timestamp, '%Y') = '2020'
            ORDER BY country, data_coleta;

	# 3- Considerando o resultado anterior, qual país teve maior índice de poluição geral por NO2? Por quê?
    
    # O Coeficiente de Variação (CV) é uma medida estatística da dispersão dos dados em uma série de dados em torno da média. 
	# O Coeficiente de Variação representa a razão entre o desvio padrão e a média e é uma estatística útil para comparar o grau 
	# de variação de uma série de dados para outra, mesmo que as médias sejam drasticamente diferentes umas das outras.
	# Quanto maior o Coeficiente de Variação , maior o nível de dispersão em torno da média, logo, maior variabilidade.
	# O Coeficiente de Variação é calculado da seguinte forma: CV = (Desvio Padrão / Média) * 100
    
	SELECT country AS pais, 
		   ROUND(AVG(no2),2) AS media_poluicao_no2, 
		   STDDEV(no2) AS desvio_padrao_no2, 
		   MAX(no2) AS valor_maximo_no2, 
		   MIN(no2) AS valor_minimo_no2,
		   (STDDEV(no2) / ROUND(AVG(no2),2)) * 100 AS cv_no2
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
	WHERE country IN ('PL', 'GB', 'NO', 'IN', 'ES')
	GROUP BY country
	ORDER BY media_poluicao_no2 DESC;
    
    SELECT country AS pais, 
		   ROUND(AVG(o3),2) AS media_poluicao_no2, 
		   STDDEV(o3) AS desvio_padrao_no2, 
		   MAX(o3) AS valor_maximo_no2, 
		   MIN(o3) AS valor_minimo_no2,
		   (STDDEV(no2) / ROUND(AVG(o3),2)) * 100 AS cv_no2
	FROM project_openaq.TB_GLOBAL_QUALITY_AIR_ETL
	WHERE country IN ('PL', 'GB', 'NO', 'IN', 'ES')
	GROUP BY country
	ORDER BY media_poluicao_no2 DESC;
    