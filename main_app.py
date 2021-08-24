from flask import Flask, render_template
import os
 
app = Flask (__name__)

import pandas as pd
import pymysql
def run_query(query):
    conn=pymysql.connect(host='dms.healthians.co.in',port=int(3306),user='mukesh_kumar',passwd='Pi85l&Y!9e&XqbP!dleBfcLd',db="testenv_prod")
    return pd.read_sql_query(query,conn)

facebook_data = '''SELECT
xcreated,
SUM(Lead_count) 'Leads',
SUM(Booking_count)'Booking_count',
SUM(Karma) 'Karma',
ROUND(SUM(Booking_count)/SUM(Lead_count),3) 'Lead_to_booking%',
ROUND(SUM(Karma)/SUM(Booking_count),0) 'Booking_ASP',
SUM(Fulfilled_orders) 'Fulfilled_orders',
SUM(Fulfilled_Revenue) 'Fulfilled_Revenue',
ROUND(SUM(Fulfilled_orders)/SUM(Lead_count),3) 'Lead_to_Fulfilled%',
ROUND(SUM(Fulfilled_Revenue)/SUM(Fulfilled_orders),0) 'Fulfilled_ASP',
ROUND(SUM(FB_Cost)*1.18,0) 'FB_Cost',
ROUND((SUM(FB_Cost)*1.18)/SUM(Lead_count),0) 'CPL',
ROUND(SUM(Fulfilled_Revenue)/(SUM(FB_Cost)*1.18),2) 'ROI'


FROM

(SELECT DATE(x3.created_on) 'xcreated',

IF(city_check_final IN ('Central Delhi','Delhi','East Delhi','Faridabad','Ghaziabad','Greater Noida',
'Gurgaon','NCR','ncr','New Delhi','Noida','North Delhi',
'South Delhi','Unspecified','West Delhi'),'NCR',
IF(city_check_final IN ('Jaipur','jaipur','JAIPUR','Jaipur Zone','RJ'),'Jaipur',
IF(city_check_final IN (' Bengaluru',' bengaluru','bangalore','Bangalore','Bangalore ',
'Bengaluru','bengaluru','BENGALURU','Bengaluru Zone 1-Kalyan Nagar','Bengaluru Zone 2-Jayanagar',
'Bengaluru Zone 3-Marathahalli','Girinagar','Mysore','mysuru','Mysuru'),'Karnataka',
IF(city_check_final IN ('Hyderabad','hyderabad','HYDERABAD'),'Hyderabad',
IF(city_check_final IN ('Bhopal','bhopal','BHOPAL','Indore','indore','INDORE','MP'),'MP',
IF(city_check_final IN ('Ambernath','Badlapur','Bhiwandi','Dombivli','Kalyan','Mira Bhayandar','mumabi',
'Mumbai','mumbai','MUMBAI','Navi Mumbai','Ulhasnagar','Vasai'),'Mumbai',
IF(city_check_final IN ('Pimpri-Chinchwad','Pune','pune','PUNE','Pune Zone 1','Pune Zone 2'),'Pune',
IF(city_check_final IN ('CHANDIGA','Chandigarh','chandigarh','CHANDIGARH','Chandigarh Zone',
'chnadigarh','Mohali','Panchkula','PCM','Sahibzada Ajit Singh Nagar'),'PCM',
IF(city_check_final IN ('Agra','agra','AGRA','Kanpur','kanpur','KANPUR','kanpur a','Kanpur Zone1','kanpur/shuklaganj','kanpur/unnao/shuklaganj','kanpur?unnao/shuklaganj','Lucknow','lucknow','LUCKNOW',
'Lucknow Zone1','Meerut','meerut','MEERUT','Meerut1','Shuklaganj','Unnao','UP','up'),'UP',
IF(city_check_final IN ('DEHRAD','Dehradun','dehradun','DEHRADUN','dheradun','UK'),'Dehradun',
IF(city_check_final IN ('Amritsar','amritsar','AMRITSAR','amritsar lang','Amritsar Lang:hi','Amritsar Lang:Hin','Amritsar Lang:Pa','AMRITSAR/JALANDH','amritsar/jalandhar','AMRITSAR/JALANDHAR','amritsar/jalndhar','AMRITSAR/JALNDHAR','Amritsir','AMRITSIR','Baddi','Jalandhar','jalandhar','JALANDHAR','LUDHIA','Ludhiana','ludhiana','LUDHIANA',
'Patiala','patiala','PATIALA','PUNJ','Punjab','punjab','PUNJAB','Sangrur','Shimla'),'Punjab',
IF(city_check_final IN ('AMBA','Ambala','Ambala Cantt','BAHADURGA','bahadurgarh','Harayana','Haryana','haryana','HARYANA','Karnal',
'KURUKSHET','Kurukshetra','Mullana','PANIP','Panipat','ROHT','Rohtak','Sonipat'),'Haryana',
 IF(city_check_final IN ('Ahmedabad','ahmedabad','AHMEDABAD','guj','gujarat','Gujarat','surat','Surat',
 'SURAT','vadodara','Vadodara','VADODARA','vadodra','VADODRA'),'Gujarat',
 IF(city_check_final IN ('Nagpur','NAGPUR','nagpur'),'Nagpur',
                          NULL)))))))))))))) City_name,
                           

##COUNT(DISTINCT x3.lead_id) 'Lead_count',
IF(Bookings = 0,0,0) 'Lead_count',
SUM(Bookings) 'Booking_count',
SUM(Book_amt) 'Karma',
SUM(Pick_orders) 'Fulfilled_orders',
SUM(Rev) 'Fulfilled_Revenue',
IF(Bookings = 0,0,0) 'FB_cost'

FROM
(
SELECT x1.lead_id,
Bookings,
Book_amt,
Pick_orders,
Rev,

##Total_Bookings,Booking_Amount,

 IF(IF(IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),
  IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))IS NULL, x5.billing_cust_city,
    IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))) IS NULL,LEFT(x1.utm_campaign,LOCATE('-',x1.utm_campaign)-1),
   
    IF(IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))IS NULL, x5.billing_cust_city,
    IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))))
    AS city_check_final,

## IF(x1.lead_source IN (145) AND lang_locale_code = 'mr','ValueDirect','Others')'lead_tag',

x1.created_on
FROM

(SELECT DISTINCT lal.lead_id,ls.id,lal.lead_city_name,MAX(lal.created_on) 'created_on',
ls.source_name, dc.city_name,lal.lead_source,lal.ad_name,lal.utm_campaign
FROM lead_activity_log AS lal
LEFT JOIN
lead_master AS lm ON lm.lead_id = lal.lead_id
LEFT JOIN
lead_source AS ls ON ls.id = lal.lead_source
LEFT JOIN
deal_city AS dc ON lal.lead_city = dc.city_id
LEFT JOIN
crm_users AS cu ON cu.id = lal.agent_id
LEFT JOIN
crm_users AS cu1 ON cu1.id = cu.reporting_to
LEFT JOIN
dept_master AS dm ON dm.id = cu.dept_id
WHERE DATE(lal.created_on) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
AND lal.is_new_interest=1
AND lal.lead_source IN (3)
AND lal.first_name NOT LIKE "%test%"
AND lal.disposition NOT IN (41)
GROUP BY lal.lead_id
ORDER BY lal.lead_id,lal.created_on DESC) x1

LEFT JOIN

(SELECT x2.lead_id,x2.billing_cust_city,MAX(x2.order_date) 'xmaxdate',
SUM(Total_Bookings) 'Bookings',
SUM(Booking_Amount) 'Book_amt',
SUM(Picked_Orders) 'Pick_orders',
SUM(Revenue) 'Rev'
FROM

(SELECT dom.order_id,dom.order_date,
dom.booked_from,dom.billing_cust_tel,dom.billing_cust_city,dom.sample_collection_time,
dom.delivery_status,dom.created_by,dom.ref_booking_id,dom.lead_id,

IF(dom.ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3),1,0) 'Total_Bookings',

IF(ref_booking_id = 0 AND dom.delivery_status IN (0,-1,3),0,
IF(ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3),dom.order_price,0)) `Booking_Amount`,

IF((is_sample_collected = 1 AND channel_type=0),1,0) 'Picked_Orders',

IF(is_sample_collected = 1 AND channel_type=0, payed_amount,0) 'Revenue'

FROM deal_order_management dom
##LEFT JOIN deal_orders_child AS doc ON dom.order_id = doc.order_id
WHERE DATE(dom.order_date) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE() AND dom.booked_from = 'crm'
AND sample_collection_time>=order_date
##AND dom.ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3)
GROUP BY dom.order_id) x2
GROUP BY x2.lead_id)x5

ON (x1.lead_id = x5.lead_id)

AND DATE(x5.xmaxdate) >= DATE(x1.created_on)
GROUP BY x1.lead_id,city_check_final
##AND city_check_final IS NOT NULL
ORDER BY x1.created_on DESC
) AS x3
GROUP BY DATE(x3.created_on),City_name

UNION

SELECT DATE(lcd.date_time) 'xcreated',
SUBSTRING(lcd.campaign_name,3,LOCATE(',',lcd.campaign_name,1)-3)
AS 'City_name',

IF(sms_rate =0,0,0) 'Lead_count',
IF(sms_rate =0,0,0) 'Booking_count',
IF(sms_rate =0,0,0) 'Karma',
IF(sms_rate =0,0,0) 'Fulfilled_orders',
IF(sms_rate =0,0,0) 'Fulfilled_Revenue',


ROUND(SUM(sms_rate),2) AS 'FB_cost'
FROM `lead_campaign_detail` lcd
WHERE lcd.vendor_id IN (3) AND DATE(Date_time) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
GROUP BY City_name,xcreated

UNION

SELECT DATE(mas.xcreated) 'xcreated',
mas.city_check_final AS 'City_name',
COUNT(DISTINCT mas.lead_id) AS 'Lead_count',
IF(mas.lead_id = 0,0,0) 'Booking_count',
IF(mas.lead_id = 0,0,0) 'Karma',
IF(mas.lead_id = 0,0,0) 'Fulfilled_orders',
IF(mas.lead_id = 0,0,0) 'Fulfilled_Revenue',
IF(mas.lead_id = 0,0,0) 'FB_cost'


 FROM
(
SELECT  DISTINCT
IF(IF(IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),
IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))IS NULL, dom.billing_cust_city,
 IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))) IS NULL,LEFT(lal.utm_campaign,LOCATE('-',lal.utm_campaign)-1),
 
 IF(IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))IS NULL, dom.billing_cust_city,
 IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))))
 AS city_check_final,
 IF(lal.lead_source IN (3,4,118,122), "Premium leads","Non Premium leads") AS lead_tag,
 IF(lal.lead_source = 3,"Facebook",IF(lal.lead_source IN (4,69,118,122),"Google",
 IF(lal.lead_source IN (143,1,14,19,20,34,37,57,58,59,60,63,119,127,145,49),"Organic_sources","Others")))'Google_Facebook',

lal.lead_id,dom.order_id,lal.lead_source,dc.city_name,DATE(lal.created_on) 'xcreated',ad_name
FROM lead_activity_log AS lal
LEFT JOIN
lead_master AS lm ON lm.lead_id = lal.lead_id
LEFT JOIN
deal_order_management AS dom ON dom.billing_cust_tel = lm.contact_number AND DATE(dom.order_date) >= DATE(lal.created_on)
LEFT JOIN
lead_source AS ls ON ls.id = lal.lead_source
LEFT JOIN
deal_city AS dc ON lal.lead_city = dc.city_id
LEFT JOIN
crm_users AS cu ON cu.id = lal.agent_id
WHERE DATE(lal.created_on) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
AND lal.lead_source IN (3)
AND lal.is_new_interest=1
AND lal.disposition NOT IN (41)
) AS mas
##WHERE city_check_final IS NOT NULL
GROUP BY xcreated,City_name
ORDER BY xcreated,City_name

)x4

GROUP BY xcreated
ORDER BY xcreated DESC'''

google_data = '''SELECT
xcreated,
SUM(Lead_count) 'Leads',
SUM(Booking_count)'Booking_count',
SUM(Karma) 'Karma',
ROUND(SUM(Booking_count)/SUM(Lead_count),3) 'Lead_to_booking%',
ROUND(SUM(Karma)/SUM(Booking_count),0) 'Booking_ASP',
SUM(Fulfilled_orders) 'Fulfilled_orders',
SUM(Fulfilled_Revenue) 'Fulfilled_Revenue',
ROUND(SUM(Fulfilled_orders)/SUM(Lead_count),3) 'Lead_to_Fulfilled%',
ROUND(SUM(Fulfilled_Revenue)/SUM(Fulfilled_orders),0) 'Fulfilled_ASP',
ROUND(SUM(Google_Cost)*1.18,0) 'Google_Cost_incl_IB',
ROUND((SUM(Google_Cost)*1.18)/SUM(Lead_count),0) 'CPL',
ROUND(SUM(Fulfilled_Revenue)/(SUM(Google_Cost)*1.18),2) 'ROI'


FROM

(SELECT DATE(x3.created_on) 'xcreated',

IF(city_check_final IN ('Central Delhi','Delhi','East Delhi','Faridabad','Ghaziabad','Greater Noida',
'Gurgaon','NCR','ncr','New Delhi','Noida','North Delhi',
'South Delhi','Unspecified','West Delhi'),'NCR',
IF(city_check_final IN ('Jaipur','jaipur','JAIPUR','Jaipur Zone','RJ'),'Jaipur',
IF(city_check_final IN (' Bengaluru',' bengaluru','bangalore','Bangalore','Bangalore ',
'Bengaluru','bengaluru','BENGALURU','Bengaluru Zone 1-Kalyan Nagar','Bengaluru Zone 2-Jayanagar',
'Bengaluru Zone 3-Marathahalli','Girinagar','Mysore','mysuru','Mysuru'),'Karnataka',
IF(city_check_final IN ('Hyderabad','hyderabad','HYDERABAD'),'Hyderabad',
IF(city_check_final IN ('Bhopal','bhopal','BHOPAL','Indore','indore','INDORE','MP'),'MP',
IF(city_check_final IN ('Ambernath','Badlapur','Bhiwandi','Dombivli','Kalyan','Mira Bhayandar','mumabi',
'Mumbai','mumbai','MUMBAI','Navi Mumbai','Ulhasnagar','Vasai'),'Mumbai',
IF(city_check_final IN ('Pimpri-Chinchwad','Pune','pune','PUNE','Pune Zone 1','Pune Zone 2'),'Pune',
IF(city_check_final IN ('CHANDIGA','Chandigarh','chandigarh','CHANDIGARH','Chandigarh Zone',
'chnadigarh','Mohali','Panchkula','PCM','Sahibzada Ajit Singh Nagar'),'PCM',
IF(city_check_final IN ('Agra','agra','AGRA','Kanpur','kanpur','KANPUR','kanpur a','Kanpur Zone1','kanpur/shuklaganj','kanpur/unnao/shuklaganj','kanpur?unnao/shuklaganj','Lucknow','lucknow','LUCKNOW',
'Lucknow Zone1','Meerut','meerut','MEERUT','Meerut1','Shuklaganj','Unnao','UP','up'),'UP',
IF(city_check_final IN ('DEHRAD','Dehradun','dehradun','DEHRADUN','dheradun','UK'),'Dehradun',
IF(city_check_final IN ('Amritsar','amritsar','AMRITSAR','amritsar lang','Amritsar Lang:hi','Amritsar Lang:Hin','Amritsar Lang:Pa','AMRITSAR/JALANDH','amritsar/jalandhar','AMRITSAR/JALANDHAR','amritsar/jalndhar','AMRITSAR/JALNDHAR','Amritsir','AMRITSIR','Baddi','Jalandhar','jalandhar','JALANDHAR','LUDHIA','Ludhiana','ludhiana','LUDHIANA',
'Patiala','patiala','PATIALA','PUNJ','Punjab','punjab','PUNJAB','Sangrur','Shimla'),'Punjab',
IF(city_check_final IN ('AMBA','Ambala','Ambala Cantt','BAHADURGA','bahadurgarh','Harayana','Haryana','haryana','HARYANA','Karnal',
'KURUKSHET','Kurukshetra','Mullana','PANIP','Panipat','ROHT','Rohtak','Sonipat'),'Haryana',
 IF(city_check_final IN ('Ahmedabad','ahmedabad','AHMEDABAD','guj','gujarat','Gujarat','surat','Surat',
 'SURAT','vadodara','Vadodara','VADODARA','vadodra','VADODRA'),'Gujarat',
 IF(city_check_final IN ('Nagpur','NAGPUR','nagpur'),'Nagpur',
                          NULL)))))))))))))) City_name,
                           

##COUNT(DISTINCT x3.lead_id) 'Lead_count',
IF(Bookings = 0,0,0) 'Lead_count',
SUM(Bookings) 'Booking_count',
SUM(Book_amt) 'Karma',
SUM(Pick_orders) 'Fulfilled_orders',
SUM(Rev) 'Fulfilled_Revenue',
IF(Bookings = 0,0,0) 'Google_cost'

FROM
(
SELECT x1.lead_id,
Bookings,
Book_amt,
Pick_orders,
Rev,

##Total_Bookings,Booking_Amount,

 IF(IF(IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),
  IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))IS NULL, x5.billing_cust_city,
    IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))) IS NULL,LEFT(x1.utm_campaign,LOCATE('-',x1.utm_campaign)-1),
   
    IF(IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR  
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))IS NULL, x5.billing_cust_city,
    IF(x1.lead_source IN (8,13,14,18,36,37), 'NCR',
 IF((IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) IS NULL OR
  IF(x1.lead_city_name = '', x1.city_name,x1.lead_city_name) = '') AND
  x1.ad_name IS NOT NULL,
  TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(x1.ad_name,'L:',-1)),',',1)),IF(x1.lead_city_name = '', x1.city_name,
    x1.lead_city_name)))))
    AS city_check_final,

## IF(x1.lead_source IN (145) AND lang_locale_code = 'mr','ValueDirect','Others')'lead_tag',

x1.created_on
FROM

(SELECT DISTINCT lal.lead_id,ls.id,lal.lead_city_name,lal.created_on,
ls.source_name, dc.city_name,lal.lead_source,lal.ad_name,lal.utm_campaign
FROM lead_activity_log AS lal
LEFT JOIN
lead_master AS lm ON lm.lead_id = lal.lead_id
LEFT JOIN
lead_source AS ls ON ls.id = lal.lead_source
LEFT JOIN
deal_city AS dc ON lal.lead_city = dc.city_id
LEFT JOIN
crm_users AS cu ON cu.id = lal.agent_id
LEFT JOIN
crm_users AS cu1 ON cu1.id = cu.reporting_to
LEFT JOIN
dept_master AS dm ON dm.id = cu.dept_id
WHERE DATE(lal.created_on) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
AND lal.is_new_interest=1
AND lal.lead_source IN (4,69,118,122,181)
AND lal.first_name NOT LIKE "%test%"
GROUP BY lal.lead_id
ORDER BY lal.lead_id,lal.created_on) x1

LEFT JOIN

(SELECT x2.lead_id,x2.billing_cust_city,MAX(x2.order_date) 'xmaxdate',
SUM(Total_Bookings) 'Bookings',
SUM(Booking_Amount) 'Book_amt',
SUM(Picked_Orders) 'Pick_orders',
SUM(Revenue) 'Rev'
FROM

(SELECT dom.order_id,dom.order_date,
dom.booked_from,dom.billing_cust_tel,dom.billing_cust_city,dom.sample_collection_time,
dom.delivery_status,dom.created_by,dom.ref_booking_id,dom.lead_id,

IF(dom.ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3),1,0) 'Total_Bookings',

IF(ref_booking_id = 0 AND dom.delivery_status IN (0,-1,3),0,
IF(ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3),dom.order_price,0)) `Booking_Amount`,

IF((is_sample_collected = 1 AND channel_type=0),1,0) 'Picked_Orders',

IF(is_sample_collected = 1 AND channel_type=0, payed_amount,0) 'Revenue'

FROM deal_order_management dom
##LEFT JOIN deal_orders_child AS doc ON dom.order_id = doc.order_id
WHERE DATE(dom.order_date) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE() AND dom.booked_from = 'crm'
AND sample_collection_time>=order_date
##AND dom.ref_booking_id = 0 AND dom.delivery_status NOT IN (0,-1,3)
GROUP BY dom.order_id) x2
GROUP BY x2.lead_id)x5

ON (x1.lead_id = x5.lead_id)

AND DATE(x5.xmaxdate) >= DATE(x1.created_on)
GROUP BY x1.lead_id,city_check_final
## AND city_check_final IS NOT NULL
) AS x3
GROUP BY DATE(x3.created_on),City_name

UNION

SELECT DATE(lcd.date_time) 'xcreated',
SUBSTRING(lcd.campaign_name,3,LOCATE(',',lcd.campaign_name,1)-3)
AS 'City_name',

IF(sms_rate =0,0,0) 'Lead_count',
IF(sms_rate =0,0,0) 'Booking_count',
IF(sms_rate =0,0,0) 'Karma',
IF(sms_rate =0,0,0) 'Fulfilled_orders',
IF(sms_rate =0,0,0) 'Fulfilled_Revenue',


ROUND(SUM(sms_rate),2) AS 'Google_cost'
FROM `lead_campaign_detail` lcd
WHERE lcd.vendor_id IN (4,69,118,122,181) AND DATE(Date_time) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
GROUP BY City_name,xcreated

UNION

SELECT DATE(mas.xcreated) 'xcreated',
mas.city_check_final AS 'City_name',
COUNT(DISTINCT mas.lead_id) AS 'Lead_count',
IF(mas.lead_id = 0,0,0) 'Booking_count',
IF(mas.lead_id = 0,0,0) 'Karma',
IF(mas.lead_id = 0,0,0) 'Fulfilled_orders',
IF(mas.lead_id = 0,0,0) 'Fulfilled_Revenue',
IF(mas.lead_id = 0,0,0) 'Google_cost'


 FROM
(
SELECT  DISTINCT
IF(IF(IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),
IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))IS NULL, dom.billing_cust_city,
 IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))) IS NULL,LEFT(lal.utm_campaign,LOCATE('-',lal.utm_campaign)-1),
 
 IF(IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))IS NULL, dom.billing_cust_city,
 IF(lal.lead_source IN (8,13,14,18,36,37), 'NCR',
IF((IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) IS NULL OR  
IF(lal.lead_city_name = '', dc.city_name,lal.lead_city_name) = '') AND
ad_name IS NOT NULL,
TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(ad_name,'L:',-1)),',',1)),IF(lal.lead_city_name = '', dc.city_name,
 lal.lead_city_name)))))
 AS city_check_final,
 IF(lal.lead_source IN (3,4,118,122), "Premium leads","Non Premium leads") AS lead_tag,
 IF(lal.lead_source = 3,"Facebook",IF(lal.lead_source IN (4,69,118,122,181),"Google",
 IF(lal.lead_source IN (143,1,14,19,20,34,37,57,58,59,60,63,119,127,145,49),"Organic_sources","Others")))'Google_Facebook',

lal.lead_id,dom.order_id,lal.lead_source,dc.city_name,DATE(lal.created_on) 'xcreated',ad_name
FROM lead_activity_log AS lal
LEFT JOIN
lead_master AS lm ON lm.lead_id = lal.lead_id
LEFT JOIN
deal_order_management AS dom ON dom.billing_cust_tel = lm.contact_number AND DATE(dom.order_date) >= DATE(lal.created_on)
LEFT JOIN
lead_source AS ls ON ls.id = lal.lead_source
LEFT JOIN
deal_city AS dc ON lal.lead_city = dc.city_id
LEFT JOIN
crm_users AS cu ON cu.id = lal.agent_id
WHERE DATE(lal.created_on) BETWEEN CURDATE()-INTERVAL 100 DAY AND CURDATE()
AND lal.lead_source IN (4,69,118,122,181)
AND lal.is_new_interest=1
AND lal.disposition NOT IN (41)
) AS mas
##WHERE city_check_final IS NOT NULL
GROUP BY xcreated,City_name
ORDER BY xcreated,City_name

)x4

GROUP BY xcreated
ORDER BY xcreated DESC
'''
addon_data = '''SELECT

 Booked_RM_name 'RM_Name',
                   
                   
                          ROUND(SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),99,                                                                  
                          0))))))),0) 'Allocated amt',
                                   
                     
         
                     SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
                     IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
                     IF((is_sample_collected=1 AND channel_user NOT IN (92)
                     AND channel_type<>0 AND payed_amount>0),payed_amount,0))))'Fulfilled Rev',
                   

                     
                     SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
                     AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0))
                     AS 'Upgrade addons',
                     
                     SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
                     AND payed_amount > allocated_amount),payed_amount - allocated_amount,
                     IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))-
                     SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
                     AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0)) 'New addons',
                     
                     
                     SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
                     AND payed_amount > allocated_amount),payed_amount - allocated_amount,
                     IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))
                     'Total addons',
                                 
 

                         
                 
                     SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),1,                                                                  
    0))))))) 'Allocated count',
 
                         
SUM(IF((is_sample_collected = 1 AND channel_type=0),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND channel_user IN (92,191)),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND booked_from IN ('Netmeds')),1,
IF((is_sample_collected=1 AND channel_user NOT IN (92,191) AND booked_from NOT IN ('Netmeds')
AND channel_type<>0 AND payed_amount>0),1,0))))) 'Fulfilled Orders',

    (SUM(IF((is_sample_collected = 1 AND channel_type=0),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND channel_user IN (92,191)),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND booked_from IN ('Netmeds')),1,
IF((is_sample_collected=1 AND channel_user NOT IN (92,191) AND booked_from NOT IN ('Netmeds')
AND channel_type<>0 AND payed_amount>0),1,0))))) /
  SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),1,                                                                  
    0))))))))'Booking Fulfillment%',
   
                      ROUND(SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
                     IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
                     IF((is_sample_collected=1 AND channel_user NOT IN (92)
                     AND channel_type<>0 AND payed_amount>0),payed_amount,0))))/
                          SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),99,                                                                  
                          0))))))),3) 'Revenue Fulfillment%',
   
   ROUND(SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
   AND payed_amount > allocated_amount),payed_amount - allocated_amount,
   IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'Total sales corrections%',
   
   ROUND(SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
   AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'Upgrade addon%',
   
   ROUND((SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
   AND payed_amount > allocated_amount),payed_amount - allocated_amount,
   IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))-
   SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
   AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0)))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'New addon%'
   
                               
                     FROM
                     
                     (SELECT d.billing_cust_tel,d.is_sample_collected, d.sample_collecor_id,allocated_amount,billing_cust_city,
                     GROUP_CONCAT(DISTINCT u.contact_number), d.order_id, d.order_price, d.payed_amount, d.sample_collection_time,
                     d.order_date, d.delivery_status,d.booked_from,d.created_by,d.channel_type,od.order_delivery_status,od.addon_flag,od.source,
                     IF(d.booked_from = 'crm',cu.name,IF(d.booked_from = 'phlebo_app',sc.name,0))
                     AS 'Agent_name',IF(d.booked_from = 'crm',cu1.name, IF(d.booked_from = 'phlebo_app',cu2.name,0)) AS 'TL_Name',
                     IF(d.booked_from = 'crm',dm.dept_name,IF(d.booked_from='phlebo_app',dm1.dept_name,0)) AS 'Dept_Name',

                 
                     camp_id,od.cancelled_date, d.reshedule_date,d.channel_user,
                     sc1.name 'Collecting_Phlebo_Name',
                     cr.name 'Collecting_RM_Name',
                     scz.zone_name 'Zone_Name',
                     IF(d.booked_from = 'phlebo_app',sc.name,sc1.name) 'Booked_phlebo_name',
                     IF(d.booked_from = 'phlebo_app',cu2.name,cr.name) 'Booked_RM_name'
           
                     
                     
                     FROM deal_order_management d
                     LEFT JOIN deal_orders_child doc ON d.order_id = doc.order_id
                     LEFT JOIN order_details od ON doc.c_order_id = od.doc_id
                     LEFT JOIN deal_user_management u ON doc.cust_id = u.user_id
                     LEFT JOIN crm_users AS cu ON d.created_by = cu.id
                     LEFT JOIN sample_collector sc ON d.created_by = sc.sid
                     LEFT JOIN sample_collector sc1 ON d.sample_collecor_id = sc1.sid
                     LEFT JOIN crm_users cr ON sc1.supervisorID = cr.id
                     LEFT JOIN sample_collection_zone scz ON scz.zone_id = sc1.zone_id
                     LEFT JOIN crm_users AS cu1 ON cu1.id = cu.reporting_to
                     LEFT JOIN crm_users AS cu2 ON sc.supervisorID = cu2.id
                     LEFT JOIN dept_master AS dm ON dm.id = cu.dept_id
                     LEFT JOIN dept_master AS dm1 ON dm1.id = sc.dept_id
                     WHERE DATE(d.sample_collection_time) = CURDATE() and camp_id = 0
                     GROUP BY d.order_id HAVING  TL_Name NOT IN ('Mudit')) X
                     GROUP BY RM_Name HAVING RM_Name IS NOT NULL  ##AND RM_name NOT LIKE '%vishal singh%'
                     
                     UNION
                     
                     SELECT

  'Total',                  
                          ROUND(SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),99,                                                                  
                          0))))))),0) 'Allocated amt',
                                   
                     
         
                     SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
                     IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
                     IF((is_sample_collected=1 AND channel_user NOT IN (92)
                     AND channel_type<>0 AND payed_amount>0),payed_amount,0))))'Fulfilled Rev',
                   

                     
                     SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
                     AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0))
                     AS 'Upgrade sales corrections',
                     
                     SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
                     AND payed_amount > allocated_amount),payed_amount - allocated_amount,
                     IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))-
                     SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
                     AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0)) 'New sales corrections',
                     
                     
                     SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
                     AND payed_amount > allocated_amount),payed_amount - allocated_amount,
                     IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))
                     'Total corrections',
                                 
 

                         
                 
                     SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),1,                                                                  
    0))))))) 'Allocated count',
 
                         
SUM(IF((is_sample_collected = 1 AND channel_type=0),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND channel_user IN (92,191)),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND booked_from IN ('Netmeds')),1,
IF((is_sample_collected=1 AND channel_user NOT IN (92,191) AND booked_from NOT IN ('Netmeds')
AND channel_type<>0 AND payed_amount>0),1,0))))) 'Fulfilled Orders',

    (SUM(IF((is_sample_collected = 1 AND channel_type=0),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND channel_user IN (92,191)),1,
IF((is_sample_collected = 1 AND channel_type<>0 AND booked_from IN ('Netmeds')),1,
IF((is_sample_collected=1 AND channel_user NOT IN (92,191) AND booked_from NOT IN ('Netmeds')
AND channel_type<>0 AND payed_amount>0),1,0))))) /
  SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
   AND channel_user NOT IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
   AND channel_user IN (92),1,
   IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),1,                                                                  
    0))))))))'Booking Fulfillment%',
   
                      ROUND(SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
                     IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
                     IF((is_sample_collected=1 AND channel_user NOT IN (92)
                     AND channel_type<>0 AND payed_amount>0),payed_amount,0))))/
                          SUM(IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)
                          AND channel_user NOT IN (92),allocated_amount,
                          IF(sample_collecor_id >0 AND delivery_status = 3 AND sample_collection_time < cancelled_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status = 13 AND sample_collection_time < reshedule_date
                          AND channel_user IN (92),99,
                          IF(sample_collecor_id >0 AND delivery_status NOT IN (3,13)AND channel_user IN (92),99,                                                                  
                          0))))))),3) 'Revenue Fulfillment%',
   
   ROUND(SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
   AND payed_amount > allocated_amount),payed_amount - allocated_amount,
   IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'Total sales corrections%',
   
   ROUND(SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
   AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'Upgrade sales corrections%',
   
   ROUND((SUM(IF((booked_from != 'phlebo_app' AND is_sample_collected=1
   AND payed_amount > allocated_amount),payed_amount - allocated_amount,
   IF((booked_from = 'phlebo_app' AND is_sample_collected=1),payed_amount,0)))-
   SUM(IF(payed_amount> allocated_amount AND booked_from!= 'Phlebo_app'
   AND payed_amount>0 AND allocated_amount>0, payed_amount - allocated_amount, 0)))/
   SUM(IF((is_sample_collected = 1 AND channel_type=0), payed_amount,
   IF((is_sample_collected = 1 AND channel_user = 92 AND channel_type <>0),99,
   IF((is_sample_collected=1 AND channel_user NOT IN (92)
   AND channel_type<>0 AND payed_amount>0),payed_amount,0)))),3) 'New sales corrections%'
   
                                 
                     FROM
                     
                     (SELECT d.billing_cust_tel,d.is_sample_collected, d.sample_collecor_id,allocated_amount,billing_cust_city,
                     GROUP_CONCAT(DISTINCT u.contact_number), d.order_id, d.order_price, d.payed_amount, d.sample_collection_time,
                     d.order_date, d.delivery_status,d.booked_from,d.created_by,d.channel_type,od.order_delivery_status,od.addon_flag,od.source,
                     IF(d.booked_from = 'crm',cu.name,IF(d.booked_from = 'phlebo_app',sc.name,0))
                     AS 'Agent_name',IF(d.booked_from = 'crm',cu1.name, IF(d.booked_from = 'phlebo_app',cu2.name,0)) AS 'TL_Name',
                     IF(d.booked_from = 'crm',dm.dept_name,IF(d.booked_from='phlebo_app',dm1.dept_name,0)) AS 'Dept_Name',

                 
                     camp_id,od.cancelled_date, d.reshedule_date,d.channel_user,
                     sc1.name 'Collecting_Phlebo_Name',
                     cr.name 'RM_Name',
                     scz.zone_name 'Zone_Name'
           
                     
                     
                     FROM deal_order_management d
                     LEFT JOIN deal_orders_child doc ON d.order_id = doc.order_id
                     LEFT JOIN order_details od ON doc.c_order_id = od.doc_id
                     LEFT JOIN deal_user_management u ON doc.cust_id = u.user_id
                     LEFT JOIN crm_users AS cu ON d.created_by = cu.id
                     LEFT JOIN sample_collector sc ON d.created_by = sc.sid
                     LEFT JOIN sample_collector sc1 ON d.sample_collecor_id = sc1.sid
                     LEFT JOIN crm_users cr ON sc1.supervisorID = cr.id
                     LEFT JOIN sample_collection_zone scz ON scz.zone_id = sc1.zone_id
                     LEFT JOIN crm_users AS cu1 ON cu1.id = cu.reporting_to
                     LEFT JOIN crm_users AS cu2 ON sc.supervisorID = cu2.id
                     LEFT JOIN dept_master AS dm ON dm.id = cu.dept_id
                     LEFT JOIN dept_master AS dm1 ON dm1.id = sc.dept_id
                     WHERE DATE(d.sample_collection_time) = CURDATE() and camp_id = 0
                     GROUP BY d.order_id) X
    WHERE RM_Name IS NOT NULL ##AND RM_name NOT LIKE '%vishal singh%'
             AND TL_Name NOT IN ('Mudit')
'''
nps_data = '''SELECT RM_Name,
SUM(IF(rating=1,1,0)) 'Rating_1',
SUM(IF(rating=2,1,0)) 'Rating_2',
SUM(IF(rating=3,1,0)) 'Rating_3',
SUM(IF(rating=4,1,0)) 'Rating_4',
SUM(IF(rating=5,1,0)) 'Rating_5',
COUNT(*) 'Total',

(SUM(IF(rating=5,1,0)) - (SUM(IF(rating=1,1,0))+ SUM(IF(rating=2,1,0))+ SUM(IF(rating=3,1,0))))
/(SUM(1))
'NPS'

FROM(

SELECT pf.booking_id, DATE(pf.created_at) AS xdate,sc.name AS Phlebo_name, sc.emp_code, cu.name AS RM_Name,
scz.zone_name, billing_cust_city, pf.rating
 FROM phlebo_feedback AS pf
LEFT JOIN deal_order_management AS dom ON dom.order_id =pf.booking_id
LEFT JOIN sample_collector AS sc ON dom.sample_collecor_id = sc.sid
LEFT JOIN locality_management AS lm ON lm.locality_id =dom.locality_id
LEFT JOIN sample_collection_zone AS scz ON scz.zone_id = lm.zone_id
LEFT JOIN crm_users AS cu ON sc.supervisorID = cu.id AND cu.status =1
LEFT JOIN nps_feedback AS nps ON nps.booking_id = pf.booking_id
WHERE DATE(pf.created_at) = CURDATE()
GROUP BY pf.booking_id)X1
GROUP BY RM_Name
'''

ota_data='''SELECT RM_name,

SUM(1) AS 'Total Bookings',

SUM(1)- SUM(IF(min_marked_time > min_slot_time,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts=0,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts<2 AND answered_calls=0,1,
0)))) 'On-Time arrival bookings',


SUM(IF(min_marked_time > min_slot_time,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts=0,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts<2 AND answered_calls=0,1,
0)))) 'Late arrival bookings',

 
1-ROUND(SUM(IF(min_marked_time > min_slot_time,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts=0,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts<2 AND answered_calls=0,1,
0))))/
SUM(1),3) 'On-time arrival%',


SUM(IF(min_marked_time > min_slot_time,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts=0,1,
IF(delivery_status = 26 AND min_marked_time < min_slot_time AND phlebo_attempts<2 AND answered_calls=0,1,
0)))) /
SUM(1)'late arrival%'

FROM
(
SELECT xdate,xtime,xhour,order_id,Phlebo_name,RM_Name,Zone_Name,min_slot_time,min_marked_time,billing_cust_city,
##IF(SUM(phlebo_attempts)=0,1,IF(SUM(phlebo_attempts)<2 AND answered_calls = 0,1,0)) 'phlebo_attempts_new',
delivery_status,
SUM(phlebo_attempts) 'phlebo_attempts',SUM(answered_calls) 'answered_calls'

FROM(

SELECT xdate,xtime,xhour,order_id,Phlebo_name,RM_Name,Zone_Name, MIN(slot_start_datetime) 'min_slot_start_datetime',
MIN(slot_start_time) min_slot_time,MIN(marked_time) min_marked_time,billing_cust_city
FROM

(SELECT DATE(d.sample_collection_time) xdate, RIGHT(TIME(d.sample_collection_time),8) xtime, HOUR(d.sample_collection_time) AS xhour,
d.order_id, s.name Phlebo_name, cr.name RM_Name, d.booked_from, lm.locality,
z.zone_name 'Zone_Name', sample_collecor_id,changed_by, billing_cust_city,
os.stage_name,
IF(d.delivery_status = 3 AND od.cancelled_date < d.sample_collection_time,0,IF(d.delivery_status =3 AND
od.cancelled_date > d.sample_collection_time,1,1)) 'cancel_tag',
IF(d.delivery_status = 13 AND d.reshedule_date < d.sample_collection_time,0,IF(d.delivery_status = 13 AND
d.reshedule_date > d.sample_collection_time,1,1)) 'reshedule_tag',
IFNULL(CONCAT(stm.date,' ',stm.time), d.sample_collection_time)  'slot_start_datetime',
IFNULL(stm.time, RIGHT(d.sample_collection_time,8))  slot_start_time, stm.end_time,
RIGHT((o.date),8) marked_time, cu.name AS RM,
IF(od.parameter_id<>0,parameter_id,IF(od.profile_id<>0,profile_id,IF(od.package_id<>0,package_id,0))) 'test_id'
FROM deal_order_management d
LEFT JOIN sample_collector s ON d.sample_collecor_id = s.sid
LEFT JOIN crm_users cr ON s.supervisorID = cr.id
LEFT JOIN locality_management lm ON d.locality_id = lm.locality_id
LEFT JOIN sample_collection_zone z ON lm.zone_id = z.zone_id
LEFT JOIN order_stage os ON d.delivery_status = os.order_stage_id
LEFT JOIN sample_time_management stm ON d.order_id = stm.booking_id
LEFT JOIN order_tracking o ON d.order_id = o.booking_id AND o.changed_status = 6 ##AND o.current_status !=26
LEFT JOIN crm_users AS cu ON s.supervisorID = cu.id
LEFT JOIN order_details AS od ON od.dom_booking_id = d.order_id
WHERE s.name IS NOT NULL
AND DATE(d.sample_collection_time) BETWEEN CURDATE() AND CURDATE()
AND d.sample_collecor_id > 0
AND DATE(d.order_date) != DATE(d.sample_collection_time)
GROUP BY xdate,Phlebo_name,xtime,marked_time##,billing_cust_city,
,order_id
HAVING cancel_tag+reshedule_tag>1 ##AND test_id NOT IN (2394)
ORDER BY xdate,Phlebo_name,xtime,marked_time ASC
) z1
GROUP BY xdate,Phlebo_name,RM_name ##,billing_cust_city
ORDER BY xdate,Phlebo_name)z2

LEFT JOIN

(SELECT ccd.lead_id,ccd.lead_contact_number,IF(call_id>0,1,0) 'Phlebo_attempts',
COUNT(DISTINCT IF(sc.status = 1 AND dept_id IN (10),call_id,0)) 'CS_call_time',
DATE(ccd.start_time) call_date,ccd.start_time,
sc.name 'collecting_phlebo_name',ccd.booking_id,dom.delivery_status,
IF(TIMESTAMPDIFF(SECOND, ccd.start_time, ccd.end_time)>10,1,0) 'answered_calls'

FROM customer_calldetails AS ccd
LEFT JOIN sample_collector AS sc ON sc.uuid = ccd.agentid
LEFT JOIN deal_order_management dom ON dom.order_id = ccd.booking_id AND DATE(dom.sample_collection_time) BETWEEN CURDATE() AND CURDATE()
WHERE DATE(ccd.start_time) BETWEEN CURDATE() AND CURDATE() ## BEGIN DATE

##AND DID IN (911244332504)
AND campaign_name LIKE '%phlebo_number_masking%'
AND ccd.type = 'manual'
AND dom.delivery_status IN (26)
GROUP BY ccd.booking_id,ccd.start_time)z3
ON z2.order_id = z3.booking_id
AND z2.Phlebo_name = z3.collecting_phlebo_name
AND DATE(z3.start_time) = xdate
AND z3.start_time < z2.min_slot_start_datetime
GROUP BY xdate,Phlebo_name,RM_name
ORDER BY xdate,Phlebo_name
LIMIT 100000000
)z4
GROUP BY RM_Name
'''


@app.route('/facebook_rolling', methods=['GET', 'POST'])
def facebook_rolling():
    table = run_query(facebook_data)
    return render_template("index.html", data=table.to_html())

@app.route('/google_rolling', methods=['GET', 'POST'])
def google_rolling():
    table = run_query(google_data)
    return render_template("index.html", data=table.to_html())

@app.route('/addon_performance', methods=['GET', 'POST'])
def addon_performance():
    table = run_query(addon_data)
    return render_template("index.html", data=table.to_html())

@app.route('/today_nps', methods=['GET', 'POST'])
def today_nps():
    table = run_query(nps_data)
    return render_template("index.html", data=table.to_html())

@app.route('/today_ota', methods=['GET', 'POST'])
def today_ota():
    table = run_query(ota_data)
    return render_template("index.html", data=table.to_html())


if __name__ == "__main__":
    app.run()   