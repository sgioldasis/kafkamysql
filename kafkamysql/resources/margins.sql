DROP TABLE IF EXISTS `Margins`
;

CREATE TABLE IF NOT EXISTS `Margins` (
    created_hour        DATETIME,
    ad_type             VARCHAR(255),
    payment_type        VARCHAR(255),
    margin        		DECIMAL(10,2)
)
;


DROP PROCEDURE IF EXISTS db.sp_Margin
;

create procedure sp_Margin()
begin
	INSERT INTO Margins (created_hour , ad_type , payment_type , margin )
	SELECT 
		STR_TO_DATE(CONCAT(SUBSTRING(created_at , 1, 10) , " ", SUBSTRING(created_at , 12, 2), ':00:00') , '%Y-%m-%d %H:00:00') as created_hour ,
		ad_type , 
		payment_type , 
		AVG( (price - payment_cost) * 100 / price ) as margin 
	FROM Classifieds
	WHERE 
		DATE_FORMAT(CONCAT(SUBSTRING(created_at , 1, 10) , " ", SUBSTRING(created_at , 12, 2), ':00:00') , '%Y-%m-%d %H:00:00') > (SELECT IFNULL(max(created_hour),'1000-01-01 00:00:00') FROM Margins) 
		AND 
		DATE_FORMAT(CONCAT(SUBSTRING(created_at , 1, 10) , " ", SUBSTRING(created_at , 12, 2), ':00:00') , '%Y-%m-%d %H:00:00') <= DATE_FORMAT(DATE_SUB(NOW(),INTERVAL 1 HOUR), '%Y-%m-%d %H:00:00')
	GROUP BY STR_TO_DATE(CONCAT(SUBSTRING(created_at , 1, 10) , " ", SUBSTRING(created_at , 12, 2), ':00:00') , '%Y-%m-%d %H:00:00'), ad_type , payment_type
	;	
end
;
    

CALL sp_Margin()
;

SET GLOBAL event_scheduler = ON
;

CREATE EVENT IF NOT EXISTS event_hourly_margins
ON SCHEDULE EVERY 1 HOUR
STARTS CURRENT_TIMESTAMP
DO CALL sp_Margin()
;

SHOW events
;
  
DROP EVENT IF EXISTS event_hourly_margins
;
     
  
