USE CS779_LabHub_final;
GO

INSERT INTO core.UserRole
    (UserRoleName)
VALUES
    ('Admin'),
    ('LabManager'),
    ('Researcher'),
    ('VendorRep');

INSERT INTO core.Department
    (DepartmentName)
VALUES
    ('Biology'),
    ('Chemistry'),
    ('Engineering'),
    ('Chemistry'),
    ('Physics'),
    ('Computer Science'),
    ('Mathematics'),
    ('Medicine'),
    ('Pharmacy'),
    ('Environmental Science');


INSERT INTO core.UnitOfMeasure
    (UnitName)
VALUES
    ('unit'),
    ('L'),
    ('g'),
    ('mol'),
    ('pack'),
    ('set');

INSERT INTO core.ProductCategory
    (CategoryName)
VALUES
    ('Equipment'),
    ('Chemical'),
    ('Consumable');

SELECT *
FROM core.ProductCategory

