# Gold Layer Data Catalog

## Overview
The Gold Layer represents business-level, clean, and enriched data designed to support analytical, reporting, and decision-making use cases. It includes **dimension tables** and **fact tables** that capture key business entities and metrics.

---

### 1. **gold.dim_customers**
**Purpose:** Contains detailed customer information, enriched with demographic and geographic attributes for analytical use.

| Column Name      | Data Type     | Description                                                        |
|------------------|---------------|--------------------------------------------------------------------|
| customer_key     | INT           | Surrogate key uniquely identifying each customer record.           |
| customer_id      | INT           | System-assigned unique numeric identifier.                         |
| customer_number  | NVARCHAR(50)  | Alphanumeric code for tracking and external referencing.           |
| first_name       | NVARCHAR(50)  | Customer's given name.                                             |
| last_name        | NVARCHAR(50)  | Customer's family or surname.                                       |
| country          | NVARCHAR(50)  | Country of residence (e.g., 'Australia').                          |
| marital_status   | NVARCHAR(50)  | Marital status (e.g., 'Married', 'Single').                        |
| gender           | NVARCHAR(50)  | Gender (e.g., 'Male', 'Female', 'n/a').                             |
| birthdate        | DATE          | Date of birth, formatted as YYYY-MM-DD (e.g., 1971-10-06).         |
| create_date      | DATE          | Timestamp when the customer record was created.                    |

---

### 2. **gold.dim_products**
**Purpose:** Captures product details and attributes for inventory, categorization, and reporting purposes.

| Column Name         | Data Type     | Description                                                        |
|---------------------|---------------|--------------------------------------------------------------------|
| product_key         | INT           | Surrogate key uniquely identifying each product.                   |
| product_id          | INT           | Internal unique identifier.                                        |
| product_number      | NVARCHAR(50)  | Alphanumeric code for categorization and inventory.                |
| product_name        | NVARCHAR(50)  | Descriptive name including type, color, or size.                   |
| category_id         | NVARCHAR(50)  | Identifier linking to the high-level category.                     |
| category            | NVARCHAR(50)  | Broad classification (e.g., Bikes, Components).                    |
| subcategory         | NVARCHAR(50)  | Specific classification within the category (e.g., Mountain).     |
| maintenance_required| NVARCHAR(50)  | Indicates if maintenance is required ('Yes', 'No').                |
| cost                | INT           | Base cost or acquisition price.                                     |
| product_line        | NVARCHAR(50)  | Product series or line (e.g., Road, Mountain).                     |
| start_date          | DATE          | Date when product became available.                                 |

---

### 3. **gold.fact_sales**
**Purpose:** Captures transactional sales events, linking customers and products for analytics.

| Column Name     | Data Type     | Description                                                        |
|-----------------|---------------|--------------------------------------------------------------------|
| order_number    | NVARCHAR(50)  | Unique identifier for each sales order (e.g., 'SO54496').          |
| product_key     | INT           | Foreign key linking to product dimension.                           |
| customer_key    | INT           | Foreign key linking to customer dimension.                          |
| order_date      | DATE          | Date the order was placed.                                          |
| shipping_date   | DATE          | Date the order was shipped.                                         |
| due_date        | DATE          | Date when payment is expected.                                      |
| sales_amount    | INT           | Total sale amount for the line item (e.g., 25).                     |
| quantity        | INT           | Number of units ordered (e.g., 1).                                 |
| price           | INT           | Unit price for the line item (e.g., 25).                            |
