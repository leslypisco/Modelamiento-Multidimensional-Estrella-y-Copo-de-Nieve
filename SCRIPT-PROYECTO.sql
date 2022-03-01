--##################################################################
-----------------------------------TIME-----------------------------

-- dim_time
--Crear dim_time
CREATE TABLE dim_time (
    TimeId    INTEGER  PRIMARY KEY AUTOINCREMENT
                       NOT NULL
                       UNIQUE,
    date      DATETIME,
    YearId    INTEGER  REFERENCES dim_year (YearId),
    Quarter   INTEGER,
    Month     INTEGER,
    Week      INTEGER,
    Day       INTEGER,
    DayWeekId INTEGER  REFERENCES dim_dayweek (DayWeekId) 
);
-- ETL dim_time
SELECT dim_time.date,
       dim_year.YearId,
       dim_time.Quarter,
       dim_time.Month,
       dim_time.Week,
       dim_time.Day,
       dim_dayweek.DayWeekId
FROM dim_time INNER JOIN dim_year
ON dim_time.Year= dim_year.Year INNER JOIN dim_dayweek
ON dim_time.DayName=dim_dayweek.DayName;



-- dim_year
--crear dim_year
CREATE TABLE dim_year (
    YearId INTEGER PRIMARY KEY AUTOINCREMENT
                   NOT NULL
                   UNIQUE,
    Year   INTEGER
);
--ETL dim_year
SELECT dim_time.Year 
FROM dim_time
GROUP BY dim_time.Year;


-- dim_dayweek

--Crear dim_dayweek
CREATE TABLE dim_dayweek (
    DayWeekId INTEGER      PRIMARY KEY AUTOINCREMENT
                           NOT NULL
                           UNIQUE,
    DayWeek   INTEGER,
    DayName   VARCHAR (15));

-- ETL dim_dayweek
SELECT  dim_time.DayWeek,
        dim_time.DayName 
FROM dim_time
GROUP BY dim_time.DayWeek ;

--################################################################################
------------------------------------ CUSTOMER-------------------------------------

-- dim_customer
--Crear dim_customer
CREATE TABLE dim_customer (
    CustomerId     NVARCHAR (5)   PRIMARY KEY
    NOT NULL      UNIQUE,
    CompanyName    NVARCHAR (100) NOT NULL,
    ContactName    NVARCHAR (40)  NOT NULL,
    ContactTitleId INTEGER        
    REFERENCES dim_contactTitle (ContactTitleId),
    AddressId      INTEGER        NOT NULL
                                  
REFERENCES dim_address (AddressId),
    Phone          NVARCHAR (24)  NOT NULL);

-- ETL dim_customer
SELECT dim_customer.CustomerId,
       dim_customer.CompanyName,
       dim_customer.ContactName,
       dim_contactTitle.ContactTitleId,
       dim_address.AddressId,
       dim_customer.Phone
FROM dim_customer INNER JOIN dim_contactTitle
ON dim_customer.ContactTitle=dim_contactTitle.ContactTitle
INNER JOIN dim_address
ON dim_customer.Address= dim_address.Address;


-- dim_contactTitle
--Crear dim_contactTitle
CREATE TABLE dim_contactTitle (
    ContactTitleId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL       UNIQUE,
    ContactTitle   NVARCHAR (40));

-- ETL dim_contactTitle
SELECT dim_customer.ContactTitle 
FROM dim_customer
GROUP BY dim_customer.ContactTitle;

-- dim_addressC
-- Crear dim_addressC
CREATE TABLE dim_address (
    AddressId  INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Address    NVARCHAR (100) NOT NULL,
    PostalCode NVARCHAR (25),
    CityId     INTEGER        
    REFERENCES dim_city (CityId) );

-- ETL dim_addressC
SELECT dim_customer.Address,
       dim_customer.PostalCode,
       dim_city.CityId
FROM dim_customer INNER JOIN dim_city
ON dim_customer.City= dim_city.City
GROUP BY dim_customer.Address;


-- dim_city
-- Crear dim_city
CREATE TABLE dim_city (
    CityId   INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    City     NVARCHAR (50) NOT NULL,
    RegionId INTEGER REFERENCES dim_region (RegionId) 
);
-- ETL dim_city
SELECT dim_customer.City,
       dim_region.RegionId
FROM dim_customer INNER JOIN dim_region
ON dim_customer.Region= dim_region.Region
GROUP BY dim_customer.City;


-- dim_region
--Crear dim_region
CREATE TABLE dim_region (
    RegionId  INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Region    NVARCHAR (30),
    CountryId INTEGER       
    REFERENCES dim_country (CountryId));

--ETL dim_region
SELECT dim_customer.Region,
       dim_country.CountryId
FROM dim_customer INNER JOIN dim_country
ON dim_customer.Country= dim_country.Country
GROUP BY dim_customer.Region;



-- dim_country
-- Crear dim_country
CREATE TABLE dim_country (
    CountryId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Country   NVARCHAR (30));

--ETL dim_country
SELECT dim_customer.Country
FROM dim_customer
GROUP BY dim_customer.Country;




--#####################################################################################
------------------------------------------ EMPLOYEE------------------------------------
-- dim_employeeH
-- Crear dim_employeeH
CREATE TABLE dim_employeeH (
    EmployeeId INTEGER    PRIMARY KEY AUTOINCREMENT
    NOT NULL   UNIQUE,
    LastName   NVARCHAR (20),
    FirstName  NVARCHAR (20),
    TitleId    INTEGER  REFERENCES dim_title (TitleId),
    BirthDate  DATETIME,
    AddressId  INTEGER  REFERENCES dim_addressE (AddressId),
    PostalCode NVARCHAR (25),
    HomePhone  NVARCHAR (24));

-- ETL dim_employee
SELECT dim_employee.LastName,
    dim_employeeH.FirstName,
    dim_title.TitleId,
    dim_employeeH.BirthDate,
    dim_addressE.AddressId,
    dim_employeeH.PostalCode,
    dim_employeeH.HomePhone
FROM dim_employeeH INNER JOIN dim_title
ON dim_employeeH.Title= dim_title.Title 
INNER JOIN dim_addressE
ON dim_employeeH.Address= dim_addressE.Address;


-- dim_title
-- Crear dim_title
CREATE TABLE dim_title (
    TitleId INTEGER  PRIMARY KEY AUTOINCREMENT
     NOT NULL  UNIQUE,
    Title      NVARCHAR (50));
-- ETL dim_title
SELECT dim_employeeH.Title 
FROM dim_employeeH 
GROUP BY dim_employeeH.Title;

-- dim_addressE
-- Crear dim_addressE   
CREATE TABLE dim_addressE (
    AddressId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Address   NVARCHAR (100) NOT NULL,
    CityId INTEGER REFERENCES dim_cityE (CityId));

-- ETL dim_addressE
SELECT  dim_employeeH.Address,
        dim_cityE.CityId
FROM dim_employeeH INNER JOIN dim_cityE
ON dim_employeeH.City= dim_cityE.City
GROUP BY dim_employeeH.Address;



--Crear dim_CityE
CREATE TABLE dim_cityE (
    CityId   INTEGER  PRIMARY KEY AUTOINCREMENT
    NOT NULL    UNIQUE,
    City     NVARCHAR (50) NOT NULL,
RegionId INTEGER REFERENCES dim_regionE(RegionId));

--ETL dim_cityE
SELECT  dim_employee.City,
        dim_regionE.RegionId
FROM dim_employee INNER JOIN dim_regionE
ON dim_employee.Region= dim_regionE.Region
GROUP BY dim_employee.City;


-- dim_region
-- Crear dim_regionE
CREATE TABLE dim_regionE (
    RegionId  INTEGER       PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Region    NVARCHAR (30),
    CountryId INTEGER REFERENCES dim_countryE(CountryId) 
);
-- ETL dim_regionE
SELECT dim_employee.Region,
       dim_countryE.CountryId
FROM dim_employee INNER JOIN dim_countryE
ON dim_employee.Country= dim_countryE.Country
GROUP BY dim_employee.Region;


-- dim_countryE
-- Crear dim_countryE
CREATE TABLE dim_countryE (
    CountryId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL  UNIQUE,
    Country    NVARCHAR (30));
--ETL dim_countryE
SELECT dim_employee.Country
FROM dim_employee
GROUP BY dim_employee.Country;


--##############################################################################
--------------------------------------ORDER------------------------------------
--dim_order_locationL
-- Crear dim_order_locationL
CREATE TABLE dim_order_locationL (
    OrderLocationId INTEGER  
    PRIMARY KEY UNIQUE,
    ShipVia         INTEGER,
    Freight         DECIMAL       NOT NULL,
    ShipNameId      INTEGER       NOT NULL
    REFERENCES dim_shipname (ShipNameId),
    ShipAddressId   INTEGER       NOT NULL
    REFERENCES dim_addressL (ShipAddressId),
    ShipPostalCode  NVARCHAR(25) NOT NULL);

--ETL dim_order_locatioL
SELECT  dim_order_location.OrderLocationId,
        dim_order_location.ShipVia,
        dim_order_location.Freight,
        dim_shipname.ShipNameId,
        dim_addressL.ShipAddressId,
        dim_order_location.ShipPostalCode 
FROM dim_order_location INNER JOIN dim_shipname
ON dim_order_location.ShipName= dim_shipName.ShipName
INNER JOIN dim_addressL
ON dim_order_location.ShipAddress= dim_addressL.ShipAddress;



--dim_shipname
-- Create dim_shipname
CREATE TABLE dim_shipname (
    ShipNameId INTEGER PRIMARY KEY UNIQUE,
    ShipName   NVARCHAR (120) NOT NULL);
-- ETL dim_shipname
SELECT dim_order_location.ShipName
FROM dim_order_location 
GROUP BY dim_order_location.ShipName;



-- dim_addressL
-- Crear dim_addressL
CREATE TABLE dim_addressL (
    ShipAddressId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    ShipAddress   NVARCHAR (100) NOT NULL,
    ShipCityId INTEGER  
    REFERENCES dim_cityL (ShipCityId));

-- ETL dim_addressL
SELECT  dim_order_location.ShipAddress,
        dim_cityL.ShipCityId
FROM dim_order_location INNER JOIN dim_cityL
ON dim_order_location.ShipCity= dim_cityL.ShipCity
GROUP BY dim_order_location.ShipAddress;



-- dim_cityL
-- Crear dim_cityL
CREATE TABLE dim_cityL (
    ShipCityId INTEGER  PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    ShipCity     NVARCHAR (50) NOT NULL,
    ShipRegionId INTEGER     
    REFERENCES dim_regionL (ShipRegionId));

--ETL dim_cityL
SELECT dim_order_location.ShipCity,
    dim_regionL.ShipRegionId
FROM dim_order_location INNER JOIN dim_regionL
ON dim_order_location.ShipRegion= dim_regionL.ShipRegion
GROUP BY dim_order_location.ShipCity;


-- dim_regionL
-- Crear dim_regionL
CREATE TABLE dim_regionL (
    ShipRegionId  INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    ShipRegion    NVARCHAR (30),
    ShipCountryId INTEGER   
    REFERENCES dim_countryL (ShipCountryId));

-- ETL dim_regionL
SELECT dim_order_location.ShipRegion,
dim_countryL.ShipCountryId
FROM dim_order_location INNER JOIN dim_countryL
ON dim_order_location.ShipCountry= dim_countryL.ShipCountry
GROUP BY dim_order_location.ShipRegion;


-- dim_countryL
-- crear dim_countryL
CREATE TABLE dim_countryL (
    ShipCountryId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    ShipCountry    NVARCHAR (30));
    
--ETL dim_countryL 
SELECT dim_order_location.ShipCountry
FROM dim_order_location
GROUP BY dim_order_location.ShipCountry;


--##########################################################################
-------------------------------------- PRODUCT -----------------------------
-- dim_productH
-- Crear dim_productH
CREATE TABLE dim_productH (
    ProductId INTEGER  PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    ProductName     NVARCHAR (80),
    QuantityPerUnit NVARCHAR (80),
    UnitPrice       DECIMAL       NOT NULL,
    UnitsInStock    INTEGER       NOT NULL,
    UnitsOnOrder    INTEGER       NOT NULL,
    CategoryId      INTEGER       
    REFERENCES dim_category (CategoryId));

--ETL dim_productH
SELECT  dim_product.ProductName,
        dim_product.QuantityPerUnit,
        dim_product.UnitPrice,
        dim_product.UnitsInStock,
        dim_product.UnitsOnOrder,
        dim_category.CategoryId 
FROM dim_product INNER JOIN dim_category
ON dim_product.CategoryName= dim_category.CategoryName;


-- dim_category
-- Crear dim_category
CREATE TABLE dim_category (
    CategoryId INTEGER PRIMARY KEY AUTOINCREMENT
    NOT NULL UNIQUE,
    CategoryName    NVARCHAR (20),
    Description     NVARCHAR (120));

-- ETL dim_category
SELECT dim_product.CategoryName,
dim_product.Description
FROM dim_product
GROUP BY dim_product.CategoryName;