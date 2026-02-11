SELECT name
FROM sys.tables;

SELECT name
FROM sys.databases;

SELECT *
FROM core.UserRole;

SELECT @@SERVERNAME;

CREATE DATABASE CS779_LabHub_final;
USE CS779_LabHub_final;

--TABLE OF CONTENT
--=================

--1. SEQUENCES
--2. USER ENTITY
--2A. FOREIGN KEYS & INDEXES for User Entities
--3. INTERNAL WORKFLOWS
--3.A FOREIGN KEYS & INDEXES for Internal Workflows
--4. SUPPLY CHAIN Entities
--4A. FOREIGN KEYS & INDEXES for SUPPLY CHAIN
--5. INVENTORY STORAGE CHAIN Entities
--5A. FOREIGN KEYS & INDEXES for INVENTORY STORAGE CHAIN
--6. LINK TABLES
--6A. FOREIGN KEY & INDEXES FOR LINK TABLES

-- ~800 line - Transactions
--1. sp_create_user_vendor_products
--TEST DATA
--2. link_products_to_supplyrequest
--TEST DATA
-- Queries
--Q1
--Q2
--Q3
-- Triggers
-- Visualizations


SELECT name, object_id, schema_id, create_date
FROM sys.tables
--=============================================================================================
DROP TABLE IF EXISTS UserSupplyRequestLink, RFBProductLink, VendorRFBLink, OrderProductLink;
DROP TABLE IF EXISTS VendorProductLink, SupplyRequestProductLink, UserNotificationLink;
DROP TABLE Location;
LocationClosure;
DROP TABLE IF EXISTS StockEvent;
DROP TABLE IF EXISTS Consumable, EquipmentItem, Chemical, InventoryItem;
DROP TABLE IF EXISTS Bid, RequestForBid, OrderHistory, OrderHistoryProductLink, Purchase, GrantOrder, EquipmentOrder, SupplyOrder, [Order];
DROP TABLE IF EXISTS Vendor, Product, UnitOfMeasure, ProductCategory;
DROP TABLE IF EXISTS SupplyRequest;
DROP TABLE IF EXISTS DocumentAssociation, DocumentEntityType, Document;
DROP TABLE IF EXISTS AuditAssociation, AuditActionType, AuditEntityType, AuditLog;
DROP TABLE IF EXISTS ApprovalEntry, ApprovalType, ApprovalRequest;
DROP TABLE IF EXISTS Notification;
DROP TABLE IF EXISTS Admin, VendorRep, Researcher, LabManager, [User];
DROP TABLE IF EXISTS Department;
DROP TABLE IF EXISTS UserRole;

-- Drop Views
DROP VIEW IF EXISTS vwRequestedProductsSummary;
DROP PROCEDURE IF EXISTS sp_create_user_vendor_products;
DROP PROCEDURE IF EXISTS sp_link_products_to_supplyrequest;
DROP TYPE IF EXISTS dbo.ProductLinkInput;
DROP TYPE IF EXISTS dbo.ProductInput;

DROP SEQUENCE IF EXISTS user_role_seq, user_seq, labmanager_seq, researcher_seq, vendorrep_seq, admin_seq, department_seq;
DROP SEQUENCE IF EXISTS notification_seq, approvalrequest_seq, approvaltype_seq, approvalentry_seq, auditlog_seq, auditassociation_seq, auditentitytype_seq, auditactiontype_seq, document_seq, documenttype_seq, documententitytype_seq, documentassociation_seq;
DROP SEQUENCE IF EXISTS supplyrequest_seq, order_seq, purchase_seq, product_seq, unitofmeasure_seq, productcategory_seq, vendor_seq, requestforbid_seq, bid_seq, order_history_seq;
DROP SEQUENCE IF EXISTS inventoryitem_seq, stockevent_seq, location_seq;
DROP SEQUENCE IF EXISTS usernotificationlink_seq, supplyrequestproductlink_seq, vendorproductlink_seq;



---------------------------------------------------------
--2. USER ENTITY ---
---------------------------------------------------------
CREATE TABLE UserRole
(
    UserRoleID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR user_role_seq),
    UserRoleName VARCHAR(64) NOT NULL
    -- Example: ('LabManager', 'Researcher', 'Admin', 'VendorRep')
);

CREATE TABLE Department
(
    DepartmentID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR department_seq),
    DepartmentName VARCHAR(255) NOT NULL
);

CREATE TABLE [User]
(
    UserID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR user_seq),
    FirstName VARCHAR(64) NOT NULL,
    LastName VARCHAR(64) NOT NULL,
    Email VARCHAR(128) NOT NULL UNIQUE,
    UserRoleID DECIMAL(12) NOT NULL,
    UserCreatedAt DATETIME2,
    UserUpdatedAt DATETIME2,
    DepartmentID DECIMAL(12) NOT NULL,
    CONSTRAINT check_EmailAtomic CHECK (Email NOT LIKE '%,%' AND Email NOT LIKE '%;%' AND Email NOT LIKE '% %')
);

CREATE TABLE LabManager
(
    LabManagerID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR labmanager_seq),
    ManageField VARCHAR(64),
    UserID DECIMAL(12) NOT NULL
);

CREATE TABLE Researcher
(
    ResearcherID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR researcher_seq),
    ResearcherEmail VARCHAR(64),
    UserID DECIMAL(12) NOT NULL
);

CREATE TABLE VendorRep
(
    VendorRepID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR vendorrep_seq),
    VendorID DECIMAL(12) NOT NULL,
    UserID DECIMAL(12) NOT NULL,
    Company VARCHAR(64)
);

CREATE TABLE Admin
(
    AdminID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR admin_seq),
    AdminField VARCHAR(64),
    UserID DECIMAL(12) NOT NULL
);

---------------------------------------------------------
--2A. FOREIGN KEYS & INDEXES for User Entities
---------------------------------------------------------
--Query-Driven Index
CREATE UNIQUE INDEX UserEmail_idx ON [User](Email);

--User
ALTER TABLE [User] ADD CONSTRAINT fk_User_UserRole FOREIGN KEY (UserRoleID) REFERENCES UserRole(UserRoleID);
CREATE INDEX idx_User_UserRoleID ON [User](UserRoleID);

ALTER TABLE [User] ADD CONSTRAINT fk_User_Department FOREIGN KEY (DepartmentID) REFERENCES Department(DepartmentID);
CREATE INDEX idx_User_DepartmentID ON [User](DepartmentID);

ALTER TABLE LabManager ADD CONSTRAINT fk_LabManager_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_LabManager_UserID ON LabManager(UserID);

ALTER TABLE Researcher ADD CONSTRAINT fk_Researcher_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_Researcher_UserID ON Researcher(UserID);

ALTER TABLE VendorRep ADD CONSTRAINT fk_VendorRep_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_VendorRep_UserID ON VendorRep(UserID);

ALTER TABLE Admin ADD CONSTRAINT fk_Admin_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_Admin_UserID ON Admin(UserID);

---------------------------------------------------------
--3. INTERNAL WORKFLOWS --
---------------------------------------------------------
CREATE TABLE Notification
(
    NotificationID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR notification_seq),
    EntityID DECIMAL(12) NOT NULL,
    Message VARCHAR(MAX),
    CreatedAt DATETIME NOT NULL,
    UpdatedAt DATETIME,
    NotificationType VARCHAR(32) NOT NULL CHECK (NotificationType IN ('Approval', 'Reminder', 'SystemAlert', 'DeliveryUpdate')),
    ApprovalEntryID DECIMAL(12)
);

---------------------------------------------------------
-- Approval Requests

CREATE TABLE ApprovalRequest
(
    ApprovalRequestID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR approvalrequest_seq),
    OrderID DECIMAL(12) NOT NULL,
    RequestorID DECIMAL(12) NOT NULL,
    ApproverID DECIMAL(12) NOT NULL,
    ApprovalDate DATETIME2
);

CREATE TABLE ApprovalType
(
    ApprovalTypeID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR approvaltype_seq),
    ApprovalTypeName VARCHAR(64) NOT NULL UNIQUE,
    Description VARCHAR(255)
);

CREATE TABLE ApprovalEntry
(
    ApprovalEntryID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR approvalentry_seq),
    ApprovalRequestID DECIMAL(12) NOT NULL,
    ApprovalTypeID DECIMAL(12) NOT NULL,
    ApprovalStatus VARCHAR(16) NOT NULL CHECK (ApprovalStatus IN ('Pending', 'Approved', 'Rejected')),
    ApprovalDate DATETIME
);

---------------------------------------------------------
-- AuditLog 

CREATE TABLE AuditLog
(
    AuditLogID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR auditlog_seq),
    AuditTime DATETIME DEFAULT CURRENT_TIMESTAMP,
    UserID DECIMAL(12) NOT NULL,
    AuditStatus VARCHAR(16) NOT NULL CHECK (AuditStatus IN ('Success', 'Failure', 'Pending'))
);

CREATE TABLE AuditEntityType
(
    AuditEntityTypeID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR auditentitytype_seq),
    ActionEntityTypeName VARCHAR(64) NOT NULL UNIQUE,
    LinkedEntityID DECIMAL(12)
);

CREATE TABLE AuditActionType
(
    AuditActionTypeID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR auditactiontype_seq),
    ActionName VARCHAR(64)
);

CREATE TABLE AuditAssociation
(
    AuditAssociationID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR auditassociation_seq),
    AuditLogID DECIMAL(12) NOT NULL,
    AuditEntityTypeID DECIMAL(12) NOT NULL,
    AuditActionTypeID DECIMAL(12) NOT NULL
);

---------------------------------------------------------
-- Document 

CREATE TABLE Document
(
    DocumentID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR document_seq),
    FileName VARCHAR(128) NOT NULL,
    FilePath VARCHAR(255),
    DocUploadedAt DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE DocumentEntityType
(
    DocumentEntityTypeID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR documententitytype_seq),
    DocumentEntityTypeName VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE DocumentAssociation
(
    DocumentAssociationID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR documentassociation_seq),
    DocumentID DECIMAL(12) NOT NULL,
    DocumentEntityTypeID DECIMAL(12) NOT NULL,
    LinkedEntityID DECIMAL(12)
);

---------------------------------------------------------
--3A. FOREIGN KEYS & INDEXES for Internal Workflows
---------------------------------------------------------

-- Notification
ALTER TABLE Notification ADD CONSTRAINT fk_Notification_ApprovalEntry FOREIGN KEY (ApprovalEntryID) REFERENCES ApprovalEntry(ApprovalEntryID);
CREATE INDEX idx_Notification_ApprovalEntryID ON Notification(ApprovalEntryID);

-- ApprovalRequest
ALTER TABLE ApprovalRequest ADD CONSTRAINT fk_ApprovalRequest_Requestor FOREIGN KEY (RequestorID) REFERENCES [User](UserID);
CREATE INDEX idx_ApprovalRequest_RequestorID ON ApprovalRequest(RequestorID);

ALTER TABLE ApprovalRequest ADD CONSTRAINT fk_ApprovalRequest_Approver FOREIGN KEY (ApproverID) REFERENCES [User](UserID);
CREATE INDEX idx_ApprovalRequest_ApproverID ON ApprovalRequest(ApproverID);

-- ApprovalEntry
ALTER TABLE ApprovalEntry ADD CONSTRAINT fk_ApprovalEntry_ApprovalRequest FOREIGN KEY (ApprovalRequestID) REFERENCES ApprovalRequest(ApprovalRequestID);
CREATE INDEX idx_ApprovalEntry_ApprovalRequestID ON ApprovalEntry(ApprovalRequestID);

ALTER TABLE ApprovalEntry ADD CONSTRAINT fk_ApprovalEntry_ApprovalType FOREIGN KEY (ApprovalTypeID) REFERENCES ApprovalType(ApprovalTypeID);
CREATE INDEX idx_ApprovalEntry_ApprovalTypeID ON ApprovalEntry(ApprovalTypeID);

-- AuditLog
ALTER TABLE AuditLog ADD CONSTRAINT fk_AuditLog_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_AuditLog_UserID ON AuditLog(UserID);

-- AuditAssociation
ALTER TABLE AuditAssociation ADD CONSTRAINT fk_AuditAssociation_AuditLog FOREIGN KEY (AuditLogID) REFERENCES AuditLog(AuditLogID);
CREATE INDEX idx_AuditAssociation_AuditLogID ON AuditAssociation(AuditLogID);

ALTER TABLE AuditAssociation ADD CONSTRAINT fk_AuditAssociation_AuditEntityType FOREIGN KEY (AuditEntityTypeID) REFERENCES AuditEntityType(AuditEntityTypeID);
CREATE INDEX idx_AuditAssociation_AuditEntityTypeID ON AuditAssociation(AuditEntityTypeID);

ALTER TABLE AuditAssociation ADD CONSTRAINT fk_AuditAssociation_AuditActionType FOREIGN KEY (AuditActionTypeID) REFERENCES AuditActionType(AuditActionTypeID);
CREATE INDEX idx_AuditAssociation_AuditActionTypeID ON AuditAssociation(AuditActionTypeID);

-- DocumentAssociation
ALTER TABLE DocumentAssociation ADD CONSTRAINT fk_DocumentAssociation_Document FOREIGN KEY (DocumentID) REFERENCES Document(DocumentID);
CREATE INDEX idx_DocumentAssociation_DocumentID ON DocumentAssociation(DocumentID);

ALTER TABLE DocumentAssociation ADD CONSTRAINT fk_DocumentAssociation_DocumentEntityType FOREIGN KEY (DocumentEntityTypeID) REFERENCES DocumentEntityType(DocumentEntityTypeID);
CREATE INDEX idx_DocumentAssociation_DocumentEntityTypeID ON DocumentAssociation(DocumentEntityTypeID);

---------------------------------------------------------
--4. SUPPLY CHAIN Entities
---------------------------------------------------------

-- Supply Requests

CREATE TABLE SupplyRequest
(
    SupplyRequestID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR supplyrequest_seq),
    NeededBy DATE,
    Status VARCHAR(16) NOT NULL CHECK (Status IN ('Draft', 'Submitted', 'Approved', 'Fulfilled', 'Cancelled')),
    CreatedAt DATETIME2 NOT NULL,
    UpdatedAt DATETIME2
);
--Query-Driven Index
CREATE INDEX SupplyRequestStatus_idx ON SupplyRequest(Status);

---------------------------------------------------------
-- Product

CREATE TABLE ProductCategory
(
    CategoryID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR productcategory_seq),
    CategoryName VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE UnitOfMeasure
(
    UnitID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR unitofmeasure_seq),
    UnitName VARCHAR(64) NOT NULL UNIQUE
);

CREATE TABLE Product
(
    ProductID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR product_seq),
    ProductName VARCHAR(128) NOT NULL,
    ProductCategoryID DECIMAL(12) NOT NULL,
    UnitID DECIMAL(12) NOT NULL,
    CreatedAt DATETIME2 NOT NULL,
    UpdatedAt DATETIME2
);
---------------------------------------------------------
-- Vendor

CREATE TABLE Vendor
(
    VendorID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR vendor_seq),
    VendorName VARCHAR(128) NOT NULL,
    Email VARCHAR(150) NOT NULL UNIQUE CHECK (Email NOT LIKE '%,%' AND Email NOT LIKE '%;%' AND Email NOT LIKE '% %'),
    VendorCreatedAt DATETIME2 NOT NULL,
    VendorStatus VARCHAR(16) NOT NULL CHECK (VendorStatus IN ('Active', 'Paused', 'Inactive'))
);

---------------------------------------------------------
-- Order
CREATE TABLE [Order]
(
    OrderID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR order_seq),
    UserID DECIMAL(12) NOT NULL,
    SupplyRequestID DECIMAL(12) NOT NULL,
    VendorID DECIMAL(12) NULL,
    Status VARCHAR(16) NOT NULL CHECK (Status IN ('Draft', 'Submitted', 'Approved', 'Fulfilled', 'Cancelled')),
    CreatedAt DATETIME2 NOT NULL,
    UpdatedAt DATETIME2
);

CREATE TABLE SupplyOrder
(
    OrderID DECIMAL(12) PRIMARY KEY NOT NULL
);

CREATE TABLE EquipmentOrder
(
    OrderID DECIMAL(12) PRIMARY KEY NOT NULL
);

CREATE TABLE GrantOrder
(
    OrderID DECIMAL(12) PRIMARY KEY NOT NULL
);

---------------------------------------------------------
-- Purchase

CREATE TABLE Purchase
(
    PurchaseID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR purchase_seq),
    OrderID DECIMAL(12) NOT NULL,
    VendorID DECIMAL(12) NOT NULL,
    Status VARCHAR(16) NOT NULL CHECK (Status IN ('Draft', 'Submitted', 'Approved', 'Fulfilled', 'Cancelled')),
    TotalAmount DECIMAL(16,2) NOT NULL,
    CreatedAt DATETIME2 NOT NULL
);

CREATE TABLE OrderHistory
(
    OrderHistoryID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR order_history_seq),
    OrderID DECIMAL(12) NOT NULL,
    PurchaseID DECIMAL(12) NOT NULL,
    ProductID DECIMAL(12) NOT NULL,
    VendorID DECIMAL(12) NOT NULL,
    UserID DECIMAL(12) NOT NULL,
    TotalAmount DECIMAL(16,2),
    UnitName VARCHAR(64),
    ProductPrice DECIMAL(12,2),
    OrderDate DATETIME2
);

CREATE TABLE RequestForBid
(
    RequestForBidID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR requestforbid_seq),
    SupplyRequestID DECIMAL(12) NOT NULL,
    InitiatorID DECIMAL(12) NOT NULL,
    RFBStatus VARCHAR(16) NOT NULL CHECK (RFBStatus IN ('Draft', 'Submitted', 'Approved', 'Fulfilled', 'Cancelled')),
    Deadline DATETIME2,
    TotalAmount DECIMAL(16,2)
);

CREATE TABLE Bid
(
    BidID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR bid_seq),
    VendorID DECIMAL(12) NOT NULL,
    RequestForBidID DECIMAL(12) NOT NULL,
    BidPrice DECIMAL(12,2) NOT NULL,
    BidAmount DECIMAL(12,2) NOT NULL,
    DeliveryCost DECIMAL(12,2),
    BidStatus VARCHAR(16) NOT NULL CHECK (BidStatus IN ('Draft', 'Submitted', 'Approved', 'Fulfilled', 'Cancelled')),
    BidCreatedAt DATETIME2 NOT NULL
);

---------------------------------------------------------
--4A. FOREIGN KEYS & INDEXES for SUPPLY CHAIN 
---------------------------------------------------------
--User -> VendorRep
ALTER TABLE VendorRep ADD CONSTRAINT fk_VendorRep_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_VendorRep_VendorID ON VendorRep(VendorID);

-- Product
ALTER TABLE Product ADD CONSTRAINT fk_Product_ProductCategory FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(CategoryID);
CREATE INDEX idx_Product_ProductCategoryID ON Product(ProductCategoryID);

ALTER TABLE Product ADD CONSTRAINT fk_Product_UnitOfMeasure FOREIGN KEY (UnitID) REFERENCES UnitOfMeasure(UnitID);
CREATE INDEX idx_Product_UnitID ON Product(UnitID);

-- [Order]
ALTER TABLE [Order] ADD CONSTRAINT fk_Order_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_Order_UserID ON [Order](UserID);

ALTER TABLE [Order] ADD CONSTRAINT fk_Order_SupplyRequest FOREIGN KEY (SupplyRequestID) REFERENCES SupplyRequest(SupplyRequestID);
CREATE INDEX idx_Order_SupplyRequestID ON [Order](SupplyRequestID);

ALTER TABLE [Order] ADD CONSTRAINT fk_Order_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_Order_VendorID ON [Order](VendorID);

ALTER TABLE ApprovalRequest ADD CONSTRAINT fk_ApprovalRequest_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
CREATE INDEX idx_ApprovalRequest_OrderID ON ApprovalRequest(OrderID);

-- SupplyOrder, EquipmentOrder, GrantOrder (specializations)
ALTER TABLE SupplyOrder ADD CONSTRAINT fk_SupplyOrder_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
CREATE INDEX idx_SupplyOrder_OrderID ON SupplyOrder(OrderID);

ALTER TABLE EquipmentOrder ADD CONSTRAINT fk_EquipmentOrder_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
CREATE INDEX idx_EquipmentOrder_OrderID ON EquipmentOrder(OrderID);

ALTER TABLE GrantOrder ADD CONSTRAINT fk_GrantOrder_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
CREATE INDEX idx_GrantOrder_OrderID ON GrantOrder(OrderID);

-- Purchase
ALTER TABLE Purchase ADD CONSTRAINT fk_Purchase_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
ALTER TABLE Purchase ADD CONSTRAINT fk_Purchase_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_Purchase_OrderID ON Purchase(OrderID);
CREATE INDEX idx_Purchase_VendorID ON Purchase(VendorID);

-- OrderHistory
ALTER TABLE OrderHistory ADD CONSTRAINT fk_OrderHistory_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
ALTER TABLE OrderHistory ADD CONSTRAINT fk_OrderHistory_Purchase FOREIGN KEY (PurchaseID) REFERENCES Purchase(PurchaseID);
ALTER TABLE OrderHistory ADD CONSTRAINT fk_OrderHistory_Vendor FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
ALTER TABLE OrderHistory ADD CONSTRAINT fk_OrderHistory_Product FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
ALTER TABLE OrderHistory ADD CONSTRAINT fk_OrderHistory_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_OrderHistory_OrderID ON OrderHistory(OrderID);
CREATE INDEX idx_OrderHistory_PurchaseID ON OrderHistory(PurchaseID);
CREATE INDEX idx_OrderHistory_ProductID ON OrderHistory(ProductID);
CREATE INDEX idx_OrderHistory_VendorID ON OrderHistory(VendorID);
CREATE INDEX idx_OrderHistory_UserID ON OrderHistory(UserID);

-- RequestForBid
ALTER TABLE RequestForBid ADD CONSTRAINT fk_RequestForBid_SupplyRequest FOREIGN KEY (SupplyRequestID) REFERENCES SupplyRequest(SupplyRequestID);
CREATE INDEX idx_RequestForBid_SupplyRequestID ON RequestForBid(SupplyRequestID);

ALTER TABLE RequestForBid ADD CONSTRAINT fk_RequestForBid_Initiator FOREIGN KEY (InitiatorID) REFERENCES [User](UserID);
CREATE INDEX idx_RequestForBid_InitiatorID ON RequestForBid(InitiatorID);

-- Bid
ALTER TABLE Bid ADD CONSTRAINT fk_Bid_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_Bid_VendorID ON Bid(VendorID);

ALTER TABLE Bid ADD CONSTRAINT fk_Bid_RequestForBid FOREIGN KEY (RequestForBidID) REFERENCES RequestForBid(RequestForBidID);
CREATE INDEX idx_Bid_RequestForBidID ON Bid(RequestForBidID);

---------------------------------------------------------
--5. INVENTORY STORAGE CHAIN Entities
---------------------------------------------------------

-- InventoryItem
CREATE TABLE InventoryItem
(
    InventoryItemID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR inventoryitem_seq),
    ProductID DECIMAL(12) NOT NULL,
    LocationID DECIMAL(12) NOT NULL,
    Category VARCHAR(64),
    ExpirationDate DATE,
    ItemAddedAt DATETIME2,
    UpdatedAt DATETIME2,
    QuantityInStock DECIMAL(12,2)
);

CREATE TABLE Chemical
(
    InventoryItemID DECIMAL(12) PRIMARY KEY NOT NULL,
    RegulationStatus VARCHAR(16) NOT NULL CHECK (RegulationStatus IN ('Hazard', 'Toxic', 'Exempt', 'Authorized', 'New')),
    SerialNumber VARCHAR(64) UNIQUE
);

CREATE TABLE EquipmentItem
(
    InventoryItemID DECIMAL(12) PRIMARY KEY NOT NULL,
    SerialNumber VARCHAR(64) UNIQUE
);

CREATE TABLE Consumable
(
    InventoryItemID DECIMAL(12) PRIMARY KEY NOT NULL,
    SerialNumber VARCHAR(64) UNIQUE
);

---------------------------------------------------------
-- StockEvent

CREATE TABLE StockEvent
(
    StockEventID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR stockevent_seq),
    InventoryItemID DECIMAL(12) NOT NULL,
    LocationID DECIMAL(12) NOT NULL,
    CurrentQuantity DECIMAL(12,2),
    NewQuantity DECIMAL(12,2),
    EventType VARCHAR(16) NOT NULL CHECK (EventType IN ('Move', 'Remove', 'Add', 'Lend')),
    EventDate DATETIME2
);

---------------------------------------------------------
-- Location

CREATE TABLE Location
(
    LocationID DECIMAL(12,0) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR location_seq),
    LocationName VARCHAR(256) NOT NULL,
    LocationType VARCHAR(16) NOT NULL CHECK (LocationType IN ('Site', 'Building', 'Room', 'Closet', 'Shelf'))
);

CREATE TABLE LocationClosure
(
    AncestorID DECIMAL(12,0) NOT NULL,
    DescendantID DECIMAL(12,0) NOT NULL,
    Depth INT NOT NULL,
    PRIMARY KEY (AncestorID, DescendantID)
);

---------------------------------------------------------
--5A. FOREIGN KEYS & INDEXES for INVENTORY STORAGE CHAIN
---------------------------------------------------------

-- InventoryItem links
ALTER TABLE InventoryItem 
    ADD CONSTRAINT fk_InventoryItem_Product FOREIGN KEY (ProductID) REFERENCES [Product](ProductID);
CREATE INDEX idx_InventoryItem_ProductID ON InventoryItem(ProductID);

ALTER TABLE InventoryItem 
    ADD CONSTRAINT fk_InventoryItem_Location FOREIGN KEY (LocationID) REFERENCES Location(LocationID);
CREATE INDEX idx_InventoryItem_LocationID ON InventoryItem(LocationID);

-- Subclass linking to InventoryItem
ALTER TABLE Chemical
    ADD CONSTRAINT fk_Chemical_InventoryItem FOREIGN KEY (InventoryItemID) REFERENCES InventoryItem(InventoryItemID);
CREATE INDEX idx_Chemical_InventoryItemID ON Chemical(InventoryItemID);

ALTER TABLE EquipmentItem
    ADD CONSTRAINT fk_EquipmentItem_InventoryItem FOREIGN KEY (InventoryItemID) REFERENCES InventoryItem(InventoryItemID);
CREATE INDEX idx_EquipmentItem_InventoryItemID ON EquipmentItem(InventoryItemID);

ALTER TABLE Consumable
    ADD CONSTRAINT fk_Consumable_InventoryItem FOREIGN KEY (InventoryItemID) REFERENCES InventoryItem(InventoryItemID);
CREATE INDEX idx_Consumable_InventoryItemID ON Consumable(InventoryItemID);

-- StockEvent linking
ALTER TABLE StockEvent 
    ADD CONSTRAINT fk_StockEvent_InventoryItem FOREIGN KEY (InventoryItemID) REFERENCES InventoryItem(InventoryItemID);
CREATE INDEX idx_StockEvent_InventoryItemID ON StockEvent(InventoryItemID);

ALTER TABLE StockEvent 
    ADD CONSTRAINT fk_StockEvent_Location FOREIGN KEY (LocationID) REFERENCES Location(LocationID);
CREATE INDEX idx_StockEvent_LocationID ON StockEvent(LocationID);

-- LocationClosure
ALTER TABLE LocationClosure
    ADD CONSTRAINT fk_LocationClosure_Ancestor FOREIGN KEY (AncestorID) REFERENCES Location(LocationID);
CREATE INDEX idx_LocationClosure_AncestorID ON LocationClosure(AncestorID);

ALTER TABLE LocationClosure 
    ADD CONSTRAINT fk_LocationClosure_Descendant FOREIGN KEY (DescendantID) REFERENCES Location(LocationID);
CREATE INDEX idx_LocationClosure_DescendantID ON LocationClosure(DescendantID);


---------------------------------------------------------
--6. LINK TABLES
---------------------------------------------------------
-- UserNotificationLink Table
CREATE TABLE UserNotificationLink
(
    UserNotificationLinkID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR usernotificationlink_seq),
    UserID DECIMAL(12) NOT NULL,
    NotificationID DECIMAL(12) NOT NULL,
    IsRead BIT NOT NULL DEFAULT 0,
    ReadAt DATETIME2
);

-- SupplyRequestProductLink Table
CREATE TABLE SupplyRequestProductLink
(
    SupplyRequestProductLinkID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR supplyrequestproductlink_seq),
    SupplyRequestID DECIMAL(12) NOT NULL,
    ProductID DECIMAL(12) NOT NULL,
    Quantity DECIMAL(8,2) NOT NULL,
    Priority VARCHAR(8) NOT NULL CHECK (Priority IN ('Low','Medium','High'))
);

--Query-Driven Index
CREATE INDEX SupplyRequestProductLinkPriority_idx ON SupplyRequestProductLink(Priority);

-- VendorProductLink Table
CREATE TABLE VendorProductLink
(
    VendorProductLinkID DECIMAL(12) PRIMARY KEY NOT NULL DEFAULT (NEXT VALUE FOR vendorproductlink_seq),
    VendorID DECIMAL(12) NOT NULL,
    ProductID DECIMAL(12) NOT NULL,
    LeadTimeDays INT,
    ProductPrice DECIMAL(12,2),
    VendorProductUpdatedAt DATETIME2
);

CREATE TABLE OrderHistoryProductLink
(
    OrderHistoryProductLinkID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR orderhistoryproductlink_seq),
    OrderHistoryID DECIMAL(12) NOT NULL,
    ProductID DECIMAL(12) NOT NULL
);

-- OrderProductLink Table
CREATE TABLE OrderProductLink
(
    OrderProductLinkID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR orderproductlink_seq),
    OrderID DECIMAL(12) NOT NULL,
    ProductID DECIMAL(12) NOT NULL
);

-- VendorRFBLink Table
CREATE TABLE VendorRFBLink
(
    VendorRFBLink DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR vendorrfblink_seq),
    VendorID DECIMAL(12) NOT NULL,
    RequestForBidID DECIMAL(12) NOT NULL,
    TotalAmount DECIMAL(16,2),
    OfferPrice DECIMAL(12,2)
);

-- RFBProductLink Table
CREATE TABLE RFBProductLink
(
    RFBProductLink DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR rfbproductlink_seq),
    ProductID DECIMAL(12) NOT NULL,
    RequestForBidID DECIMAL(12) NOT NULL
);

-- UserSupplyRequestLink Table
CREATE TABLE UserSupplyRequestLink
(
    UserSupplyRequestLinkID DECIMAL(12) PRIMARY KEY DEFAULT (NEXT VALUE FOR usersupplyrequestlink_seq),
    UserID DECIMAL(12) NOT NULL,
    SupplyRequestID DECIMAL(12) NOT NULL
);

---------------------------------------------------------
--6A. FOREIGN KEY & INDEXES FOR LINK TABLES
---------------------------------------------------------

-- UserNotificationLink
ALTER TABLE UserNotificationLink
    ADD CONSTRAINT fk_UserNotificationLink_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_UserNotificationLink_UserID ON UserNotificationLink(UserID);

ALTER TABLE UserNotificationLink
    ADD CONSTRAINT fk_UserNotificationLink_Notification FOREIGN KEY (NotificationID) REFERENCES Notification(NotificationID);
CREATE INDEX idx_UserNotificationLink_NotificationID ON UserNotificationLink(NotificationID);

-- SupplyRequestProductLink
ALTER TABLE SupplyRequestProductLink
    ADD CONSTRAINT fk_SupplyRequestProductLink_SupplyRequest FOREIGN KEY (SupplyRequestID) REFERENCES SupplyRequest(SupplyRequestID);
CREATE INDEX idx_SupplyRequestProductLink_SupplyRequestID ON SupplyRequestProductLink(SupplyRequestID);

ALTER TABLE SupplyRequestProductLink
    ADD CONSTRAINT fk_SupplyRequestProductLink_Product FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
CREATE INDEX idx_SupplyRequestProductLink_ProductID ON SupplyRequestProductLink(ProductID);

-- VendorProductLink
ALTER TABLE VendorProductLink
    ADD CONSTRAINT fk_VendorProductLink_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_VendorProductLink_VendorID ON VendorProductLink(VendorID);

ALTER TABLE VendorProductLink
    ADD CONSTRAINT fk_VendorProductLink_Product FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
CREATE INDEX idx_VendorProductLink_ProductID ON VendorProductLink(ProductID);

-- OrderProductLink
ALTER TABLE OrderProductLink
    ADD CONSTRAINT fk_OrderProductLink_Order FOREIGN KEY (OrderID) REFERENCES [Order](OrderID);
CREATE INDEX idx_OrderProductLink_OrderID ON OrderProductLink(OrderID);
ALTER TABLE OrderProductLink
    ADD CONSTRAINT fk_OrderProductLink_Product FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
CREATE INDEX idx_OrderProductLink_ProductID ON OrderProductLink(ProductID);

-- VendorRFBLink
ALTER TABLE VendorRFBLink
    ADD CONSTRAINT fk_VendorRFBLink_Vendor FOREIGN KEY (VendorID) REFERENCES Vendor(VendorID);
CREATE INDEX idx_VendorRFBLink_VendorID ON VendorRFBLink(VendorID);

ALTER TABLE VendorRFBLink
    ADD CONSTRAINT fk_VendorRFBLink_RequestForBid FOREIGN KEY (RequestForBidID) REFERENCES RequestForBid(RequestForBidID);
CREATE INDEX idx_VendorRFBLink_RequestForBidID ON VendorRFBLink(RequestForBidID);

-- RFBProductLink
ALTER TABLE RFBProductLink
    ADD CONSTRAINT fk_RFBProductLink_Product FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
CREATE INDEX idx_RFBProductLink_ProductID ON RFBProductLink(ProductID);

ALTER TABLE RFBProductLink
    ADD CONSTRAINT fk_RFBProductLink_RequestForBid FOREIGN KEY (RequestForBidID) REFERENCES RequestForBid(RequestForBidID);
CREATE INDEX idx_RFBProductLink_RequestForBidID ON RFBProductLink(RequestForBidID);

-- OrderHistoryProductLink
ALTER TABLE OrderHistoryProductLink ADD CONSTRAINT fk_OrderHistoryProductLink_OrderHistory FOREIGN KEY (OrderHistoryID) REFERENCES [OrderHistory](OrderHistoryID);
CREATE INDEX idx_OrderHistoryProductLink_OrderHistoryID ON OrderHistoryProductLink(OrderHistoryID);

ALTER TABLE OrderHistoryProductLink ADD CONSTRAINT fk_OrderHistoryProductLink_Product FOREIGN KEY (ProductID) REFERENCES Product(ProductID);
CREATE INDEX idx_OrderHistoryProductLink_ProductID ON OrderHistoryProductLink(ProductID);

-- UserSupplyRequestLink
ALTER TABLE UserSupplyRequestLink
    ADD CONSTRAINT fk_UserSupplyRequestLink_User FOREIGN KEY (UserID) REFERENCES [User](UserID);
CREATE INDEX idx_UserSupplyRequestLink_UserID ON UserSupplyRequestLink(UserID);

ALTER TABLE UserSupplyRequestLink
    ADD CONSTRAINT fk_UserSupplyRequestLink_SupplyRequest FOREIGN KEY (SupplyRequestID) REFERENCES SupplyRequest(SupplyRequestID);
CREATE INDEX idx_UserSupplyRequestLink_SupplyRequestID ON UserSupplyRequestLink(SupplyRequestID);
