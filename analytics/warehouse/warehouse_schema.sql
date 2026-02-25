--
--THIS IS THE WAREHOUSE SCHEMA FILE.
--
-- Create schema
CREATE SCHEMA IF NOT EXISTS dw;


-- Sequences for auto-incrementing keys
CREATE SEQUENCE IF NOT EXISTS dw.seq_product_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_location_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_user_key;
CREATE SEQUENCE IF NOT EXISTS dw.seq_fact_inventory_key;

-- =========================
-- Dimension: Dim_Date
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Date (
    DateKey INT PRIMARY KEY,              -- YYYYMMDD
    FullDate DATE NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(32) NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL,
    DayOfWeek VARCHAR(32) NOT NULL
);

-- =========================
-- Dimension: Dim_Product
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Product (
    ProductKey INTEGER PRIMARY KEY DEFAULT nextval('dw.seq_product_key'),
    ProductID INT NOT NULL,
    ProductName VARCHAR(128) NOT NULL,
    CategoryName VARCHAR(64) NOT NULL,
    UnitOfMeasure VARCHAR(64) NOT NULL,
    Description TEXT
);

-- =========================
-- Dimension: Dim_Location
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_Location (
    LocationKey INTEGER PRIMARY KEY DEFAULT nextval('dw.seq_location_key'),
    LocationID INT NOT NULL,
    SiteName VARCHAR(256),
    Building VARCHAR(256),
    RoomNumber VARCHAR(256),
    StorageType VARCHAR(32)
);

-- =========================
-- Dimension: Dim_User
-- =========================
CREATE TABLE IF NOT EXISTS dw.Dim_User (
    UserKey INTEGER PRIMARY KEY DEFAULT nextval('dw.seq_user_key'),
    UserID INT NOT NULL,
    UserName VARCHAR(128) NOT NULL,
    UserRole VARCHAR(64) NOT NULL,
    DepartmentName VARCHAR(255) NOT NULL
);

-- =========================
-- Fact: Fact_Inventory_Transactions
-- =========================
CREATE TABLE IF NOT EXISTS dw.Fact_Inventory_Transactions (
    TransactionID INT PRIMARY KEY,     -- From inventory.StockEvent.StockEventID
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    LocationKey INT NOT NULL,
    UserKey INT NOT NULL,

    QuantityDelta DECIMAL(12,2) NOT NULL,
    AbsoluteQuantity DECIMAL(12,2) NOT NULL,
    CurrentStockSnapshot DECIMAL(12,2) NOT NULL,
    EventType VARCHAR(32) NOT NULL,

    CONSTRAINT fk_fact_date
        FOREIGN KEY (DateKey) REFERENCES dw.Dim_Date(DateKey),

    CONSTRAINT fk_fact_product
        FOREIGN KEY (ProductKey) REFERENCES dw.Dim_Product(ProductKey),

    CONSTRAINT fk_fact_location
        FOREIGN KEY (LocationKey) REFERENCES dw.Dim_Location(LocationKey),

    CONSTRAINT fk_fact_user
        FOREIGN KEY (UserKey) REFERENCES dw.Dim_User(UserKey)
);