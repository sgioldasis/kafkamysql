CREATE TABLE IF NOT EXISTS `Classifieds` (
    id                  VARCHAR(255)  PRIMARY KEY,
    customer_id         VARCHAR(255), 
    created_at          VARCHAR(255),
    text                TEXT,
    ad_type             VARCHAR(255),
    price               DECIMAL(10,2),
    currency            VARCHAR(255),
    payment_type        VARCHAR(255),
    payment_cost        DECIMAL(10,2),
    created_dt          DATETIME(6),
    created_ns          SMALLINT
)
===

DROP TABLE IF EXISTS `Margins`
===

CREATE TABLE IF NOT EXISTS `Margins` (
    created_hour        DATETIME,
    ad_type             VARCHAR(255),
    payment_type        VARCHAR(255),
    margin        		DECIMAL(10,2)
)
===

DROP PROCEDURE IF EXISTS db.sp_Margin
===

CREATE PROCEDURE sp_Margin()
BEGIN
	INSERT INTO Margins (created_hour , ad_type , payment_type , margin )
	SELECT 
		DATE_FORMAT(created_dt, '%Y-%m-%d %H:00:00') as created_hour ,
		ad_type , 
		payment_type , 
		AVG( (price - payment_cost) * 100 / price ) as margin 
	FROM Classifieds
	WHERE 
		DATE_FORMAT(created_dt, '%Y-%m-%d %H:00:00') > (SELECT IFNULL(max(created_hour),'1000-01-01 00:00:00') FROM Margins) 
		AND 
		DATE_FORMAT(created_dt, '%Y-%m-%d %H:00:00') <= DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')
	GROUP BY DATE_FORMAT(created_dt, '%Y-%m-%d %H:00:00'), ad_type , payment_type
	;	
END
===

DROP EVENT IF EXISTS ev_margins_hourly
===

CREATE EVENT IF NOT EXISTS ev_margins_hourly
ON SCHEDULE 
EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP
DO CALL sp_Margin()
===
    
SET GLOBAL event_scheduler = ON
     
