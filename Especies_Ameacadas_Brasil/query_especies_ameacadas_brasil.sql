# Criando o Schema
CREATE SCHEMA mma;

#Criando a tabela
CREATE TABLE  mma.ESPECIE_AMEACA (
	`grupo` text,
    `subgrupo` text,
    `familia` text,
    `especie` text,
    `nome_comum` text,
    `categoria_ameaca` text,
    `sigla_categoria_ameaca` text,
    `bioma` text,
    `principais_ameacas` text,
    `presenca_areas_protegidas` text,
    `pan` text,
    `ordenamento_pesqueiro` text,
    `nivel_protecao` text,
    `especie_exclusiva_brasil` text,
    `uf_ocorrencias` text,
    `ano_lista` text
);

# Importação dos dados através do Table Data Import Wizard
# Importação datset 1
	# Setando o ano do dataset 1
	UPDATE mma.especie_ameaca
	SET ano_lista = '2018'
	WHERE ano_lista IS NULL;
# Importação datset 2
	# Setando o ano do dataset 2
	UPDATE mma.especie_ameaca
	SET ano_lista = '2019'
	WHERE ano_lista IS NULL;
# Importação datset 3
	# Setando o ano do dataset 3
	UPDATE mma.especie_ameaca
	SET ano_lista = '2020'
	WHERE ano_lista IS NULL;

# Verificando se o campo ano_lista foi preenchido
SELECT DISTINCT ano_lista FROM mma.especie_ameaca;

# Limpeza e tranformação dos dados
	#Deletando linhas vazias
		DELETE FROM mma.especie_ameaca
        WHERE sigla_categoria_ameaca = '';
        
	#Imputando valores para o campo categoria_ameaca
    UPDATE mma.especie_ameaca
    SET sigla_categoria_ameaca = CASE WHEN sigla_categoria_ameaca = 'VU' THEN 'Vulneravel'
							   WHEN sigla_categoria_ameaca = 'EN' THEN 'Em Perigo'
                               WHEN sigla_categoria_ameaca IN ('CR', 'CR(PEW)', 'CR/PEW', 'CR(PEX)', 'CR/PEX') THEN 'Criticamente em Perigo'
                               WHEN sigla_categoria_ameaca IN ('EX', 'EW') THEN 'Extinta na Natureza'
						  ELSE null END;
                          
	# Detecção e Remoção de Valores Ausentes
    
    SELECT * FROM mma.especie_ameaca WHERE nome_comum IN ('', ' ', NULL, 'Vazio');
    
    UPDATE mma.especie_ameaca
    SET nome_comum = 'Desconhecido'
    WHERE nome_comum  = 'Vazio' OR nome_comum  = '-';
    
    UPDATE mma.especie_ameaca
    SET presenca_areas_protegidas = 'Nao'
    WHERE presenca_areas_protegidas IN ('#N/D') OR presenca_areas_protegidas IS NULL;
    
    UPDATE mma.especie_ameaca
    SET pan = 'Nao'
    WHERE pan IS NULL;
    
	UPDATE mma.especie_ameaca
    SET ordenamento_pesqueiro = 'Nao'
    WHERE ordenamento_pesqueiro IS NULL;
    
    UPDATE mma.especie_ameaca
    SET nivel_protecao = 'Nao informado'
    WHERE nivel_protecao IS NULL;
    
    UPDATE mma.especie_ameaca
    SET especie_exclusiva_brasil = 'Informacao nao disponivel'
    WHERE especie_exclusiva_brasil IS NULL;
    
   #Tratando a string principais_ameacas
   
   # Padronizando o delimitador das ameaças para ponto-e-vírgula
   UPDATE mma.especie_ameaca
   SET principais_ameacas = replace(principais_ameacas, ',', ';');
   
   # Encontranto a quantidade maxima de ameaças por espécie
   SELECT MAX(round((length(principais_ameacas) - length(replace(principais_ameacas,';',''))) / length(';'))) AS max_qtd_ameaca FROM mma.especie_ameaca;
   
   # Criando uma tabela para as principais ameaças
   CREATE TABLE mma.principais_ameacas AS
	   SELECT ano_lista, especie, SUBSTRING_INDEX(principais_ameacas, ';', 1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 2), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 3), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 4), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 5), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 6), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 7), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 8), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(principais_ameacas, ';', 9), ';', -1) AS ameacas FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(principais_ameacas, ';', -1) AS ameacas FROM mma.especie_ameaca;
       
       # Deletando as linhas sem uma categoria de ameaças
       DELETE FROM mma.principais_ameacas
       WHERE ameacas IS NULL OR ameacas = 'Vazio' OR ameacas = ' ';
       
       #Limpando e tratandos os dados
       
		UPDATE mma.principais_ameacas
        SET ameacas = 'Turismo'
        WHERE ameacas LIKE '%Turismo%';

		UPDATE mma.principais_ameacas
        SET ameacas = 'Industrias'
        WHERE ameacas LIKE '%Industrias%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Extracao Direta'
        WHERE ameacas LIKE '%Extracao Direta%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Assentamento Humano'
        WHERE ameacas LIKE '%Assentamento Humano%' OR ameacas LIKE '%Assentamentos Humanos%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Mudanca Na Dinamica Das Especies Nativas'
        WHERE ameacas LIKE '%Mudanca Na Dinamica Das Especies Nativas%' OR ameacas LIKE '%Mudancas na Dinamica das Especies Nativas%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Barrages Abastecimento/Drenagem'
        WHERE ameacas LIKE '%Outras Atividades Economicas: Barrages Abastecimento/Drenagem%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Energia'
        WHERE ameacas LIKE '%Outras Atividades Economicas:Energia%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Transporte'
        WHERE ameacas LIKE '%Outras Atividades Economicas:Transporte%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = 'Agropecuaria'
        WHERE ameacas LIKE '%Agropecuaria Aquacultura%';
        
        UPDATE mma.principais_ameacas
        SET ameacas = ltrim(ameacas);

#Tratando a string bioma
   
   # Padronizando o delimitador das ameaças para ponto-e-vírgula
   UPDATE mma.especie_ameaca
   SET bioma = replace(bioma, ',', ';');
   
   # Encontranto a quantidade maxima de ameaças por espécie
   SELECT MAX(round((length(bioma) - length(replace(bioma,';',''))) / length(';'))) AS max_qtd_bioma FROM mma.especie_ameaca;
   
   # Criando uma tabela para as principais ameaças
   CREATE TABLE mma.biomas AS
	   SELECT ano_lista, especie, SUBSTRING_INDEX(bioma, ';', 1) AS bioma FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(bioma, ';', 2), ';', -1) AS bioma FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(bioma, ';', 3), ';', -1) AS bioma FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(bioma, ';', 4), ';', -1) AS bioma FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(SUBSTRING_INDEX(bioma, ';', 5), ';', -1) AS bioma FROM mma.especie_ameaca
	   UNION ALL
	   SELECT ano_lista, especie, SUBSTRING_INDEX(bioma, ';', -1) AS bioma FROM mma.especie_ameaca;
       
       # Deletando as linhas sem bioma
       DELETE FROM mma.biomas
       WHERE bioma IS NULL OR bioma = 'Vazio' OR bioma = ' ' OR bioma = '';
       
       #Limpando e tratandos os dados
        UPDATE mma.biomas
        SET bioma = ltrim(bioma);
        
		UPDATE mma.biomas
        SET bioma = 'Caatinga'
        WHERE bioma LIKE '%Caatinga%';
        
        
   
    