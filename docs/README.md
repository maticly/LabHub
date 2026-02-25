erDiagram
    DIM_PRODUCT {
        int ProductKey PK
        int ProductID
        string ProductName
        string CategoryName
        string UnitOfMeasure
        string Description
    }

    DIM_USER {
        int UserKey PK
        int UserID
        string UserName
        string Role
    }

    DIM_LOCATION {
        int LocationKey PK
        int LocationID
        string LocationName
        string Region
    }

    DIM_DATE {
        int DateKey PK
        date FullDate
        int Year
        int Month
        int Day
        string DayOfWeek
    }

    FACT_INVENTORY_TRANSACTIONS {
        int TransactionKey PK
        int ProductKey FK
        int UserKey FK
        int LocationKey FK
        int DateKey FK
        int AbsoluteQuantity
        string TransactionType
    }

    FACT_INVENTORY_TRANSACTIONS ||--|| DIM_PRODUCT : "ProductKey"
    FACT_INVENTORY_TRANSACTIONS ||--|| DIM_USER : "UserKey"
    FACT_INVENTORY_TRANSACTIONS ||--|| DIM_LOCATION : "LocationKey"
    FACT_INVENTORY_TRANSACTIONS ||--|| DIM_DATE : "DateKey"
