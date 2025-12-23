CREATE OR REFRESH STREAMING LIVE TABLE bronze.bitcoin
TBLPROPERTIES ("quality" = "bronze")  -- metadado opcional para indicar a camada
AS

-- Lê os arquivos JSON da pasta RAW usando cloud_files (Auto Loader)
SELECT *
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot/',  -- caminho de origem
  'json',                                                   -- formato
  map(
    'cloudFiles.includeExistingFiles', 'false',
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'addNewColumns'
  )
);
