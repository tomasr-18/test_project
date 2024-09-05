CREATE VIEW `tomastestproject-433206.testdb_1.avg_scores_and_stock_data` AS

-- Första CTE för att få unika rader och beräkna medelvärden
WITH ranked_rows AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY  
      pub_date, 
      title, 
      url, 
      source_name, 
      company
      ORDER BY (SELECT NULL)
    ) AS row_num
  FROM 
    `tomastestproject-433206.testdb_1.clean_news_data`
),

unique_news AS (
  SELECT 
    author, 
    description, 
    pub_date, 
    title, 
    url, 
    source_name, 
    company, 
    score_description, 
    score_title
  FROM 
    ranked_rows
  WHERE 
    row_num = 1
),

avg_scores AS (
  SELECT
    company,
    AVG(score_description) AS avg_score_description,  -- Medelvärde av score_description
    AVG(score_title) AS avg_score_title,
    DATE(pub_date) AS pub_date   -- Datum
  FROM 
    unique_news
  GROUP BY 
    company, DATE(pub_date)
),

-- CTE för att välja unika rader baserat på stock_symbol och date
unique_stocks AS (
  SELECT 
    stock_symbol, 
    date,  -- Använd original date-kolumnen utan omvandling
    open, 
    high, 
    low, 
    close, 
    volume
  FROM (
    SELECT 
      *, 
      ROW_NUMBER() OVER (PARTITION BY stock_symbol, date) AS row_num
    FROM 
      `tomastestproject-433206.testdb_1.clean_stock_data`
  )
  WHERE row_num = 1
)

-- Joina avg_scores med unique_stocks på pub_date och company
SELECT 
  a.avg_score_description, 
  a.avg_score_title, 
  b.stock_symbol AS company,  -- Matcha företaget från aktiedata
  b.date AS pub_date,         -- Matcha datum från aktiedata
  b.open, 
  b.high, 
  b.low, 
  b.close, 
  b.volume
FROM 
  unique_stocks b
RIGHT JOIN 
  avg_scores a
ON 
  a.company = b.stock_symbol
  AND a.pub_date = b.date
ORDER BY 
  b.date DESC;
