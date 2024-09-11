--aggregerad data för varje aktie

SELECT 
    company,
    -- Mean Absolute Percentage Error (MAPE)
    AVG(CASE 
            WHEN true_value != 0 THEN ABS((true_value - predicted_value) / true_value) * 100
            ELSE NULL
        END) AS mape,
    -- Mean Absolute Error (MAE)
    AVG(ABS(true_value - predicted_value)) AS mae,
    -- Mean Squared Error (MSE)
    AVG(POW(true_value - predicted_value, 2)) AS mse,
    -- Root Mean Squared Error (RMSE)
    SQRT(AVG(POW(true_value - predicted_value, 2))) AS rmse
FROM 
    `tomastestproject-433206.testdb_1.predictions`
GROUP BY 
    company;


--för varje rad
SELECT 
    company,
    model_name,
    true_value,
    predicted_value,
    date,
    -- Mean Absolute Percentage Error (MAPE)
    CASE 
        WHEN true_value != 0 THEN ABS((true_value - predicted_value) / true_value) * 100
        ELSE NULL  -- Handle division by zero
    END AS mape,
    -- Mean Absolute Error (MAE)
    ABS(true_value - predicted_value) AS mae

FROM 
    `tomastestproject-433206.testdb_1.predictions`;