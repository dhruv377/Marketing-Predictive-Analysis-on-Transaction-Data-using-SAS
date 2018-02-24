
/* importing the drug tissue dataset */ 
/* data drug1;
infile "G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\factiss_drug_1114_1165" firstobs=2 ;
input IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR; */

proc import datafile= "G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\factis_drug.csv" dbms=csv replace out=drug1 ;run;

/*converted Feature to numeric adding Fnum column*/
DATA drug;
SET drug1;
if IRI_KEY in (642166,
642583,
650679,
651444,
662928,
1091828,
8000583,
8003042,
8003043,
8003059,
8029341,
8046669);
IF F = 'A+' THEN Rebate = 1 ;
ELSE Rebate = 0;
if F='A+' then Fnum=0;
IF F = 'NONE' THEN Fnum = 0;
/* IF F = 'FS-C' THEN Fnum = 1; */
IF F = 'C' THEN Fnum = 1;
/* IF F = 'FS-B' THEN Fnum = 3; */
IF F = 'B' THEN Fnum = 2;
/* IF F = 'FS-A' THEN Fnum = 5; */
IF F = 'A' THEN Fnum = 3;
/* IF F = 'FSA+' THEN Fnum = 7; */
run;

proc print data=drug(obs=10); run;



/* imported the delivery store dataset */

proc import datafile= "G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\delivery_store.csv"  dbms=csv replace out=delivery_store ;run;

/* INPUT IRI_KEY 1-8 OU $ 9-11 EST_ACV 12-20 Market_Name $ 21-45 Open 46-50 Clsd 51-55 Masked_nm $ 56-63; */
  proc print data=delivery_store(obs=10); run;  

/* run; */

/* importing prod_tissue data */ 

proc import datafile= "G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\prod_tissue.csv" dbms=csv replace out=tissue ;run;

/* proc sql; */
/*  */
/* select L5 from tissue;run; */
/* proc print data=tissue(obs=10);run; */

/*  */
/* PROC SQL; */
/* CREATE TABLE Market_Share AS */
/* SELECT c.Market_Name, b.L5,SUM(a.DOLLARS) AS Total_Sales_brand  FROM drug a JOIN tissue b ON a.VEND=b.VEND and a.ITEM=b.ITEM  */
/* JOIN delivery_store c ON a.IRI_KEY=c.IRI_KEY where b.L5 IN ('KLEENEX','SCOTTIES', 'PUFFS')   */
/* GROUP BY c.Market_Name, b.L5; */

/* TABLE SHOWING AVERAGE PRICE, AVG FEATURE , AVG DISPLAY FOR TOP 2 BRANDS */ 

PROC SQL;
CREATE TABLE Market_Share AS
SELECT c.Market_Name, b.L5,  SUM(a.DOLLARS) AS Total_Sales_brand,SUM(a.UNITS) AS total_Units,AVG(a.D) AS Avg_Display,AVG(a.Fnum) AS Avg_Feature
,AVG(PR) AS Avg_PR,AVG(a.DOLLARS/(a.UNITS*b.VOL_EQ)) AS Avg_price  FROM drug a JOIN tissue b ON a.VEND=b.VEND AND a.ITEM=B.ITEM 
JOIN delivery_store c ON a.IRI_KEY=c.IRI_KEY where b.L5 IN ('KLEENEX','SCOTTIES', 'PUFFS') 
GROUP BY c.Market_Name,b.L5;




 DATA final_Market_Share;
SET Market_Share;
IF Week = 1114 then Month='Dec 00';
IF Week >1114 AND Week<=1118 then Month='Jan 01';
IF Week >1118 AND Week<=1122 then Month='Feb 01';
IF Week >1122 AND Week<=1126 then Month='Mar 01';
IF Week >1126 AND Week<=1131 then Month='Apr 01';
IF Week >1131 AND Week<=1135 then Month='May 01';
IF Week >1135 AND Week<=1139 then Month='Jun 01';
IF Week >1139 AND Week<=1143 then Month='Jul 01';
IF Week >1143 AND Week<=1148 then Month='Aug 01';
IF Week >1148 AND Week<=1152 then Month='Sep 01';
IF Week >1152 AND Week<=1156 then Month='Oct 01';
IF Week >1156 AND Week<=1160 then Month='Nov 01';
IF Week >1160 AND Week<=1165 then Month='Dec 01';
RUN;


PROC PRINT DATA=final_Market_Share(obs=100);run;
/* Importing drug panel data */
DATA drugpanel;
INFILE 'G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\factiss_PANEL_DR_1114_1165.DAT' expandtabs FIRSTOBS=2;
INPUT PANID WEEK UNITS OUTLET $ DOLLARS IRI_KEY COLUPC :$13.;
IF length(COLUPC) = 11 THEN Vend = substr(COLUPC,2,5);
IF length(COLUPC) = 12 THEN vend = substr(COLUPC,3,5);
IF length(COLUPC) = 13 THEN vend = substr(COLUPC,4,5);
rename PANID=PanelistID;
if vend=36000 then brand_choice=1; /* kleenex */

if vend=37000 then brand_choice=2;  /* puffs */
if vend=99998 then brand_choice=3; /* private label */
		


proc sql;
select unique IRI_KEY FROM drugpanel;run;

RUN;
PROC PRINT DATA = drugpanel(obs=100);RUN;
proc import datafile="G:\SEM_2\pred_mkt\drive-download-20170228T010624Z-001\ads demo2.csv" dbms=csv replace out=demo; run;

proc print data=demo(obs=10);run;

PROC SORT data = drugpanel; BY PanelistID; RUN;
data demo_cleaned;

MERGE drugpanel demo; by PanelistID;
IF WEEK;
IF type_of_residential_possession = 2 THEN House_Owned = 1;
ELSE House_Owned= 0;

if Income_HH=0 then delete;
if Family_Size=0 then delete;
if HH_RACE=3 THEN HH_RACE=1; ELSE HH_RACE= 0;
IF (number_of_cats ne 0 or number_of_dogs ne 0) then Pets_Owned=1; else Pets_Owned=0;
if Children_Group_Code=0 then delete;
if Children_Group_Code=8 then Children_Group_Code=0;
if marital_status=0 then delete;
if marital_Status=2 then marital_Status=1 ; else marital_status=0;

if Age_Group_Male_HH = 0 then delete;
if Age_Group_Male_HH=7 THEN Age_Group_Male_HH=0;

if Age_Group_female_HH = 0 then delete;
if Age_Group_female_HH=7 THEN Age_Group_female_HH=0;
combined_age= (Age_Group_female_HH+Age_Group_Male_HH)/2;
combined_age= round(combined_age);

if Education_Level_Female=0 then delete;
if Education_Level_Male = 0 then  delete;
if Education_Level_Female=9 then Education_Level_Female=0;
if Education_Level_Male=9 then Education_Level_Male=0;
Education_Level=(Education_Level_Male+Education_Level_Female)/2;
Education_Level=round(Education_Level);


if Male_Working_Hour_Code=99 then delete;
if Male_Working_Hour_Code=2 or Male_Working_Hour_Code=3 then Male_work=1; else Male_work=0;
if female_Working_Hour_Code=2 or female_Working_Hour_Code=3 then female_work=1; else female_work=0;




run;
proc sql;
create table facial_tissue_store as 
select IRI_KEY, week,
 CASE WHEN AVG(Price1)>0 THEN Avg(Price1) ELSE MAX(AVG(Price1),AVG(Price2),AVG(Price3))END AS Price1, 
  CASE WHEN AVG(Price2)>0 THEN Avg(Price2) ELSE MAX(AVG(Price1),AVG(Price2),AVG(Price3))END AS Price2,   
  CASE WHEN AVG(Price3)>0 THEN Avg(Price3) ELSE MAX(AVG(Price1),AVG(Price2),AVG(Price3))END AS Price3,   
    Ceil(AVG(Display1)) AS Display1, 
 Ceil(AVG(Display2)) AS Display2,
 Ceil(AVG(Display3)) AS Display3, 
  Ceil(AVG(PR1)) AS PR1,  
 Ceil(AVG(PR2)) AS PR2,  
 Ceil(AVG(PR3)) AS PR3,  
 

 Ceil(AVG(Rebate1)) AS Rebate1, 
 Ceil(AVG(Rebate2)) AS Rebate2, 
 Ceil(AVG(Rebate3)) AS Rebate3,

 Ceil(AVG(Fnum1)) AS Fnum1, 
 Ceil(AVG(Fnum2)) AS Fnum2, 
 Ceil(AVG(Fnum3)) AS Fnum3
 
from 
(
select IRI_KEY, WEEK, 

CASE when VEND=36000 THEN Avg(DOLLARS/UNITS) ELSE 0 END AS Price1, 
CASE when VEND=37000 THEN Avg(DOLLARS/UNITS) ELSE 0 END AS Price2, 
CASE when VEND=99998 THEN Avg(DOLLARS/UNITS) ELSE 0 END AS Price3, 


CASE WHEN VEND = 36000 THEN AVG(D) ELSE 0 END AS Display1,
CASE WHEN VEND = 37000 THEN AVG(D) ELSE 0 END AS Display2, 
CASE WHEN VEND = 99998 THEN AVG(D) ELSE 0 END AS Display3,


 CASE WHEN VEND = 36000 THEN AVG(PR) ELSE 0 END AS PR1,  
 CASE WHEN VEND = 37000 THEN AVG(PR) ELSE 0 END AS PR2,  
  CASE WHEN VEND = 99998 THEN AVG(PR) ELSE 0 END AS PR3,


 CASE WHEN VEND = 36000 THEN AVG(Rebate) ELSE 0 END AS Rebate1,  
 CASE WHEN VEND = 37000 THEN AVG(Rebate) ELSE 0 END AS Rebate2,  
 CASE WHEN VEND = 99998 THEN AVG(Rebate) ELSE 0 END AS Rebate3,  
 


 CASE WHEN VEND = 36000 THEN AVG(Fnum) ELSE 0 END AS Fnum1,  
CASE WHEN VEND = 37000 THEN AVG(Fnum) ELSE 0 END AS Fnum2,  
  CASE WHEN VEND = 99998 THEN AVG(Fnum) ELSE 0 END AS Fnum3 


from drug group by IRI_KEY, WEEK, VEND) GROUP BY IRI_KEY,WEEK; RUN; QUIT;


PROC SORT data = demo_cleaned; BY IRI_KEY WEEK; RUN;


data merged;
merge demo_cleaned facial_tissue_store;
by IRI_KEY WEEK;
IF PANELISTID;
if price1;
RUN;



proc print data=demo_cleaned(obs=100); run;

proc corr data=demo_cleaned;
Var House_Owned Income_HH Family_Size HH_RACE Pets_Owned Children_Group_Code marital_status  combined_age Education_Level Male_Working_Hour_Code female_Working_Hour_Code; run; 
 proc reg data= demo_cleaned;
 model  Children_Group_Code=Family_Size House_Owned Income_HH  HH_RACE Pets_Owned marital_status  combined_age Education_Level Male_Working_Hour_Code female_Working_Hour_Code/ tol vif collin;


/*  */
/*  proc sql;  */
/*  */
/*  create table drug_demo as select d1.WEEK, D1.UNITS, D1.DOLLARS, D1.IRI_KEY, D1.COLUPC, D2.Family_Size from drugpanel d1 inner join demo d2  */
/*  ON d2.PanelistID=d1.PANID;RUN; */
 
 
 
 /* data set for mdc */

data mdc_tissue; 
set merged;
array pvec{3} price1 - price3; 
array dvec{3} Display1 - Display3; 
array prvec{3} pr1 - pr3;
 array rvec{3} rebate1 - rebate3;
array fvec{3} Fnum1 - Fnum3; 
retain PID 0; PID+1;

do i = 1 to 3;
 Brand=i;
  price=pvec{i}; 
  Display=dvec{i}; 
  pr=prvec{i};
  rebate=rvec{i}; 
  Fnum=Fvec{i};
    decision=(brand_choice=i); 
    output;
      end;
 run;



/* creating 2 dummies */
data mdc_tissue;
set mdc_tissue;

br2=0;
br3=0;

if brand=2 then br2=2;
if brand=3 then br3=3;

 Income2=Income_HH*br2;
 Income3=Income_HH*br3;

 Family_Size2=Family_Size*br2;
 Family_Size3=Family_Size*br3;
 
 
 Children_Group_Code2=Children_Group_Code*br2;
 Children_Group_Code3=Children_Group_Code*br3;

 
 House_Owned2=House_Owned*br2;
 House_Owned3=House_Owned*br3;
 
 
 marital_Status2=marital_Status*br2;
marital_Status3=marital_Status*br3;

 
 
  Pets_Owned2=Pets_Owned*br2;
 Pets_Owned3=Pets_Owned*br3; 
 
 

 
 
 combined_age2=combined_age*br2;
 combined_age3=combined_age*br3;
 
 
 
 HH_RACE2=HH_RACE*br1;
 HH_RACE3=HH_RAC


br1oc export data=mdc_tissue outfile= "G:\op.csv" dbms=csv replace; run;
 proc mdc data=mdc_tissue;
 model decision=br1  
 
  / type=clogit ice=(Bbr1d 1 2 3);
 id PID;
 output out=pred_tissue pred=p;
 run;
 