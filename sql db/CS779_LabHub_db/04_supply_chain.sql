USE CS779_LabHub_final;
GO

BEGIN TRAN;
GO
CREATE SCHEMA supply;
GO

-- Supply Requests

CREATE TABLE supply.SupplyRequest
(
    SupplyRequestID BIGINT IDENTITY PRIMARY KEY,
    NeededBy DATE,
    Status VARCHAR(16) CHECK (Status IN ('Draft','Submitted','Approved','Fulfilled','Cancelled')),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

-- Order

CREATE TABLE supply.[Order]
(
    OrderID BIGINT IDENTITY PRIMARY KEY,
    UserID BIGINT NOT NULL,
    SupplyRequestID BIGINT NOT NULL,
    VendorID BIGINT,
    Status VARCHAR(16),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

-- Purchase
CREATE TABLE supply.Purchase
(
    PurchaseID BIGINT IDENTITY PRIMARY KEY,
    OrderID BIGINT NOT NULL,
    VendorID BIGINT NOT NULL,
    TotalAmount DECIMAL(16,2),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

-- OrderHistory
CREATE TABLE supply.OrderHistory
(
    OrderHistoryID BIGINT IDENTITY PRIMARY KEY,
    OrderID BIGINT NOT NULL,
    PurchaseID BIGINT,
    ProductID BIGINT,
    VendorID BIGINT,
    UserID BIGINT,
    TotalAmount DECIMAL(16,2),
    OrderDate DATETIME2
);

-- RFB / Bidding

CREATE TABLE supply.RequestForBid
(
    RequestForBidID BIGINT IDENTITY PRIMARY KEY,
    SupplyRequestID BIGINT,
    InitiatorID BIGINT,
    Status VARCHAR(16),
    Deadline DATETIME2
);

CREATE TABLE supply.Bid
(
    BidID BIGINT IDENTITY PRIMARY KEY,
    VendorID BIGINT,
    RequestForBidID BIGINT,
    BidPrice DECIMAL(12,2),
    BidStatus VARCHAR(16),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME()
);

CREATE INDEX idx_supplyrequest_status ON supply.SupplyRequest(Status);
CREATE INDEX idx_order_user ON supply.[Order](UserID);
CREATE INDEX idx_bid_vendor ON supply.Bid(VendorID);

COMMIT;
GO
