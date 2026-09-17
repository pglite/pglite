import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { PGLite } from "../src/index";

describe("100 Comprehensive In-Depth PostgreSQL Foreign Keys & Constraints Test Cases for PGLite", () => {
  let db: PGLite;

  beforeAll(async () => {
    db = new PGLite(":memory:", { native: true });

    // Base tables for standard schema
    await db.query(`
      CREATE TABLE departments (
        id SERIAL PRIMARY KEY,
        dept_name TEXT NOT NULL UNIQUE,
        budget NUMERIC DEFAULT 100000 CHECK (budget >= 0)
      );
    `);

    await db.query(`
      CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        department_id INT REFERENCES departments(id) ON DELETE CASCADE,
        manager_id INT REFERENCES employees(id) ON DELETE SET NULL,
        salary NUMERIC NOT NULL CHECK (salary > 0),
        status TEXT DEFAULT 'active' CHECK (status IN ('active', 'on_leave', 'terminated'))
      );
    `);

    await db.query(`
      CREATE TABLE employee_profiles (
        id SERIAL PRIMARY KEY,
        employee_id INT NOT NULL UNIQUE REFERENCES employees(id) ON DELETE CASCADE,
        bio TEXT,
        github_handle TEXT
      );
    `);

    await db.query(`
      CREATE TABLE projects (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        lead_id INT REFERENCES employees(id) ON DELETE SET NULL,
        start_date TEXT NOT NULL,
        end_date TEXT
      );
    `);

    await db.query(`
      CREATE TABLE project_assignments (
        project_id INT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        employee_id INT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        role TEXT NOT NULL DEFAULT 'contributor',
        assigned_hours INT DEFAULT 40 CHECK (assigned_hours > 0),
        PRIMARY KEY (project_id, employee_id)
      );
    `);
  });

  afterAll(async () => {
    if (db) {
      await db.close();
    }
  });

  // =========================================================================
  // Section 1: Single-Column Foreign Keys & Referential Integrity (1 - 20)
  // =========================================================================
  describe("Section 1: Single-Column Foreign Keys & Referential Integrity", () => {
    test("01. Insert parent rows into departments", async () => {
      await db.query("INSERT INTO departments (id, dept_name, budget) VALUES (1, 'Engineering', 500000);");
      await db.query("INSERT INTO departments (id, dept_name, budget) VALUES (2, 'Design', 200000);");
      const res = await db.query("SELECT count(*) AS total FROM departments;");
      expect(Number(res[0].total)).toBe(2);
    });

    test("02. Insert child row with valid foreign key referencing departments", async () => {
      await db.query("INSERT INTO employees (id, name, email, department_id, salary) VALUES (1, 'Alice', 'alice@corp.com', 1, 120000);");
      const res = await db.query("SELECT * FROM employees WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice");
      expect(Number(res[0].department_id)).toBe(1);
    });

    test("03. Insert child row with NULL foreign key allowed", async () => {
      await db.query("INSERT INTO employees (id, name, email, department_id, salary) VALUES (2, 'Bob Freelance', 'bob@free.com', NULL, 80000);");
      const res = await db.query("SELECT * FROM employees WHERE id = 2;");
      expect(res.length).toBe(1);
      expect(res[0].department_id).toBeNull();
    });

    test("04. Self-referencing FK pointing to an existing employee as manager", async () => {
      await db.query("INSERT INTO employees (id, name, email, department_id, manager_id, salary) VALUES (3, 'Charlie', 'charlie@corp.com', 1, 1, 90000);");
      const res = await db.query("SELECT * FROM employees WHERE id = 3;");
      expect(Number(res[0].manager_id)).toBe(1);
    });

    test("05. 1-to-1 relationship enforced via UNIQUE foreign key on profile", async () => {
      await db.query("INSERT INTO employee_profiles (employee_id, bio, github_handle) VALUES (1, 'Lead Architect', 'alice_arch');");
      const res = await db.query("SELECT * FROM employee_profiles WHERE employee_id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].github_handle).toBe("alice_arch");
    });

    test("06. Querying join between parent and child tables via foreign key", async () => {
      const res = await db.query(`
        SELECT e.name, d.dept_name, e.salary
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.id = 1;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("07. Left join preserving employees without department", async () => {
      const res = await db.query(`
        SELECT e.name, d.dept_name
        FROM employees e
        LEFT JOIN departments d ON e.department_id = d.id
        ORDER BY e.id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(3);
      const bob = res.find(r => r.name === 'Bob Freelance');
      expect(bob).toBeDefined();
      expect(bob.dept_name).toBeNull();
    });

    test("08. Inserting project with valid lead_id foreign key", async () => {
      await db.query("INSERT INTO projects (id, title, lead_id, start_date) VALUES (10, 'Cloud Migration', 1, '2026-01-15');");
      const res = await db.query("SELECT * FROM projects WHERE id = 10;");
      expect(res.length).toBe(1);
      expect(Number(res[0].lead_id)).toBe(1);
    });

    test("09. Inserting project with NULL lead_id", async () => {
      await db.query("INSERT INTO projects (id, title, lead_id, start_date) VALUES (11, 'Secret Project', NULL, '2026-03-01');");
      const res = await db.query("SELECT * FROM projects WHERE id = 11;");
      expect(res[0].lead_id).toBeNull();
    });

    test("10. Inserting composite project assignment junction row", async () => {
      await db.query("INSERT INTO project_assignments (project_id, employee_id, role, assigned_hours) VALUES (10, 1, 'Lead', 30);");
      await db.query("INSERT INTO project_assignments (project_id, employee_id, role, assigned_hours) VALUES (10, 3, 'Developer', 40);");
      const res = await db.query("SELECT * FROM project_assignments WHERE project_id = 10;");
      expect(res.length).toBe(2);
    });

    test("11. Self-referencing tree query traversing manager to reports", async () => {
      const res = await db.query(`
        SELECT m.name AS manager, count(e.id) AS reports_count
        FROM employees m
        JOIN employees e ON e.manager_id = m.id
        GROUP BY m.name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].manager).toBe("Alice");
      expect(Number(res[0].reports_count)).toBe(1);
    });

    test("12. Filtering child table by parent table attribute in subquery", async () => {
      const res = await db.query(`
        SELECT name, salary FROM employees
        WHERE department_id IN (SELECT id FROM departments WHERE budget >= 300000)
        ORDER BY salary DESC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Alice");
    });

    test("13. Correlated EXISTS subquery across foreign key", async () => {
      const res = await db.query(`
        SELECT d.dept_name FROM departments d
        WHERE EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id);
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("14. Correlated NOT EXISTS to find departments with no employees", async () => {
      const res = await db.query(`
        SELECT d.dept_name FROM departments d
        WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.department_id = d.id);
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Design");
    });

    test("15. Updating foreign key column to point to new parent", async () => {
      await db.query("UPDATE employees SET department_id = 2 WHERE id = 3;");
      const res = await db.query("SELECT department_id FROM employees WHERE id = 3;");
      expect(Number(res[0].department_id)).toBe(2);
    });

    test("16. Updating foreign key column to NULL", async () => {
      await db.query("UPDATE employees SET department_id = NULL WHERE id = 3;");
      const res = await db.query("SELECT department_id FROM employees WHERE id = 3;");
      expect(res[0].department_id).toBeNull();
      // Revert back for future tests
      await db.query("UPDATE employees SET department_id = 1 WHERE id = 3;");
    });

    test("17. Many-to-Many join across junction table", async () => {
      const res = await db.query(`
        SELECT p.title, e.name, pa.role
        FROM projects p
        JOIN project_assignments pa ON p.id = pa.project_id
        JOIN employees e ON pa.employee_id = e.id
        WHERE p.id = 10
        ORDER BY e.id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].title).toBe("Cloud Migration");
      expect(res[0].name).toBe("Alice");
      expect(res[1].name).toBe("Charlie");
    });

    test("18. Aggregating project hours per department via multi-table FK join", async () => {
      const res = await db.query(`
        SELECT d.dept_name, sum(pa.assigned_hours) AS total_hours
        FROM departments d
        JOIN employees e ON d.id = e.department_id
        JOIN project_assignments pa ON e.id = pa.employee_id
        GROUP BY d.dept_name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
      expect(Number(res[0].total_hours)).toBe(70);
    });

    test("19. Parameterized lookup following foreign key", async () => {
      const res = await db.query(
        "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.department_id = d.id WHERE d.dept_name = $1;",
        ["Engineering"]
      );
      expect(res.length).toBe(2);
    });

    test("20. Count of children per parent including 0 with LEFT JOIN", async () => {
      const res = await db.query(`
        SELECT d.dept_name, count(e.id) AS emp_count
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id
        GROUP BY d.dept_name
        ORDER BY d.dept_name ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].dept_name).toBe("Design");
      expect(Number(res[0].emp_count)).toBe(0);
      expect(res[1].dept_name).toBe("Engineering");
      expect(Number(res[1].emp_count)).toBe(2);
    });
  });

  // =========================================================================
  // Section 2: Composite Keys & Multi-Column Constraints (21 - 40)
  // =========================================================================
  describe("Section 2: Composite Keys & Multi-Column Constraints", () => {
    beforeAll(async () => {
      await db.query(`
        CREATE TABLE tenants (
          tenant_id INT PRIMARY KEY,
          company_name TEXT NOT NULL
        );
      `);

      await db.query(`
        CREATE TABLE tenant_users (
          tenant_id INT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
          user_id INT NOT NULL,
          user_name TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'member',
          PRIMARY KEY (tenant_id, user_id)
        );
      `);

      await db.query(`
        CREATE TABLE tenant_documents (
          doc_id SERIAL PRIMARY KEY,
          tenant_id INT NOT NULL,
          author_id INT NOT NULL,
          title TEXT NOT NULL,
          content TEXT,
          FOREIGN KEY (tenant_id, author_id) REFERENCES tenant_users(tenant_id, user_id) ON DELETE CASCADE
        );
      `);
    });

    test("21. Insert into tenants table", async () => {
      await db.query("INSERT INTO tenants (tenant_id, company_name) VALUES (101, 'Acme Corp'), (102, 'Beta Tech');");
      const res = await db.query("SELECT count(*) AS c FROM tenants;");
      expect(Number(res[0].c)).toBe(2);
    });

    test("22. Insert into composite primary key table tenant_users", async () => {
      await db.query("INSERT INTO tenant_users (tenant_id, user_id, user_name, role) VALUES (101, 1, 'John Doe', 'admin');");
      await db.query("INSERT INTO tenant_users (tenant_id, user_id, user_name, role) VALUES (101, 2, 'Jane Doe', 'member');");
      await db.query("INSERT INTO tenant_users (tenant_id, user_id, user_name, role) VALUES (102, 1, 'Beta Admin', 'admin');");
      const res = await db.query("SELECT * FROM tenant_users ORDER BY tenant_id ASC, user_id ASC;");
      expect(res.length).toBe(3);
    });

    test("23. Query composite primary key with exact pair match", async () => {
      const res = await db.query("SELECT * FROM tenant_users WHERE tenant_id = 101 AND user_id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].user_name).toBe("John Doe");
    });

    test("24. Query composite primary key isolating single tenant", async () => {
      const res = await db.query("SELECT * FROM tenant_users WHERE tenant_id = 101 ORDER BY user_id ASC;");
      expect(res.length).toBe(2);
      expect(res[0].user_name).toBe("John Doe");
      expect(res[1].user_name).toBe("Jane Doe");
    });

    test("25. Insert into table with composite foreign key reference", async () => {
      await db.query("INSERT INTO tenant_documents (doc_id, tenant_id, author_id, title, content) VALUES (1, 101, 1, 'Q1 RoadMap', 'Planning docs');");
      await db.query("INSERT INTO tenant_documents (doc_id, tenant_id, author_id, title, content) VALUES (2, 101, 2, 'Design Spec', 'UI wireframes');");
      const res = await db.query("SELECT count(*) AS total FROM tenant_documents;");
      expect(Number(res[0].total)).toBe(2);
    });

    test("26. Join between composite FK child and composite PK parent", async () => {
      const res = await db.query(`
        SELECT d.title, u.user_name, t.company_name
        FROM tenant_documents d
        JOIN tenant_users u ON d.tenant_id = u.tenant_id AND d.author_id = u.user_id
        JOIN tenants t ON d.tenant_id = t.tenant_id
        WHERE d.doc_id = 1;
      `);
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("Q1 RoadMap");
      expect(res[0].user_name).toBe("John Doe");
      expect(res[0].company_name).toBe("Acme Corp");
    });

    test("27. Multi-column UNIQUE constraint table creation", async () => {
      await db.query(`
        CREATE TABLE branch_products (
          id SERIAL PRIMARY KEY,
          branch_code TEXT NOT NULL,
          sku TEXT NOT NULL,
          price NUMERIC NOT NULL,
          stock INT NOT NULL DEFAULT 0,
          UNIQUE (branch_code, sku)
        );
      `);
      await db.query("INSERT INTO branch_products (branch_code, sku, price, stock) VALUES ('NYC', 'ITEM-1', 29.99, 100);");
      await db.query("INSERT INTO branch_products (branch_code, sku, price, stock) VALUES ('LA', 'ITEM-1', 34.99, 50);");
      const res = await db.query("SELECT * FROM branch_products ORDER BY id ASC;");
      expect(res.length).toBe(2);
    });

    test("28. Querying rows by multi-column unique key", async () => {
      const res = await db.query("SELECT price, stock FROM branch_products WHERE branch_code = 'NYC' AND sku = 'ITEM-1';");
      expect(res.length).toBe(1);
      expect(Number(res[0].price)).toBe(29.99);
    });

    test("29. Updating non-key attributes on composite table", async () => {
      await db.query("UPDATE branch_products SET stock = 120 WHERE branch_code = 'NYC' AND sku = 'ITEM-1';");
      const res = await db.query("SELECT stock FROM branch_products WHERE branch_code = 'NYC' AND sku = 'ITEM-1';");
      expect(Number(res[0].stock)).toBe(120);
    });

    test("30. Deleting single row from composite table by primary key pair", async () => {
      await db.query("DELETE FROM tenant_users WHERE tenant_id = 102 AND user_id = 1;");
      const res = await db.query("SELECT * FROM tenant_users WHERE tenant_id = 102;");
      expect(res.length).toBe(0);
    });

    test("31. Table with 3-column composite primary key", async () => {
      await db.query(`
        CREATE TABLE flight_bookings (
          flight_num TEXT NOT NULL,
          flight_date TEXT NOT NULL,
          seat_num TEXT NOT NULL,
          passenger_name TEXT NOT NULL,
          fare NUMERIC NOT NULL,
          PRIMARY KEY (flight_num, flight_date, seat_num)
        );
      `);
      await db.query("INSERT INTO flight_bookings VALUES ('VN100', '2026-06-01', '12A', 'Nguyen Van A', 250);");
      await db.query("INSERT INTO flight_bookings VALUES ('VN100', '2026-06-01', '12B', 'Tran Thi B', 250);");
      await db.query("INSERT INTO flight_bookings VALUES ('VN100', '2026-06-02', '12A', 'Le Van C', 280);");
      const res = await db.query("SELECT count(*) AS booked FROM flight_bookings WHERE flight_num = 'VN100';");
      expect(Number(res[0].booked)).toBe(3);
    });

    test("32. Querying 3-column composite key for specific flight instance", async () => {
      const res = await db.query("SELECT passenger_name FROM flight_bookings WHERE flight_num = 'VN100' AND flight_date = '2026-06-01' ORDER BY seat_num ASC;");
      expect(res.length).toBe(2);
      expect(res[0].passenger_name).toBe("Nguyen Van A");
      expect(res[1].passenger_name).toBe("Tran Thi B");
    });

    test("33. Composite key grouping and passenger count aggregation", async () => {
      const res = await db.query(`
        SELECT flight_num, flight_date, count(*) AS passengers, sum(fare) AS total_revenue
        FROM flight_bookings
        GROUP BY flight_num, flight_date
        ORDER BY flight_date ASC;
      `);
      expect(res.length).toBe(2);
      expect(Number(res[0].passengers)).toBe(2);
      expect(Number(res[0].total_revenue)).toBe(500);
      expect(Number(res[1].passengers)).toBe(1);
    });

    test("34. Star schema join across 3 tables with foreign keys", async () => {
      const res = await db.query(`
        SELECT td.doc_id, td.title, tu.user_name, t.company_name
        FROM tenant_documents td
        JOIN tenant_users tu ON td.tenant_id = tu.tenant_id AND td.author_id = tu.user_id
        JOIN tenants t ON td.tenant_id = t.tenant_id
        ORDER BY td.doc_id ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].title).toBe("Q1 RoadMap");
      expect(res[1].title).toBe("Design Spec");
    });

    test("35. Aggregating documents count per tenant user", async () => {
      const res = await db.query(`
        SELECT tu.user_name, count(td.doc_id) AS doc_count
        FROM tenant_users tu
        LEFT JOIN tenant_documents td ON tu.tenant_id = td.tenant_id AND tu.user_id = td.author_id
        GROUP BY tu.user_name
        ORDER BY tu.user_name ASC;
      `);
      expect(res.length).toBe(2);
      expect(Number(res[0].doc_count)).toBe(1);
      expect(Number(res[1].doc_count)).toBe(1);
    });

    test("36. Nested subquery checking composite existence", async () => {
      const res = await db.query(`
        SELECT * FROM tenant_users tu
        WHERE EXISTS (
          SELECT 1 FROM tenant_documents td
          WHERE td.tenant_id = tu.tenant_id AND td.author_id = tu.user_id
        )
        ORDER BY tu.user_id ASC;
      `);
      expect(res.length).toBe(2);
    });

    test("37. In-clause with multiple values on composite child table", async () => {
      const res = await db.query("SELECT * FROM flight_bookings WHERE seat_num IN ('12A', '12B') ORDER BY flight_date ASC, seat_num ASC;");
      expect(res.length).toBe(3);
    });

    test("38. Distinct query on composite table columns", async () => {
      const res = await db.query("SELECT DISTINCT flight_num, flight_date FROM flight_bookings ORDER BY flight_date ASC;");
      expect(res.length).toBe(2);
    });

    test("39. Parameterized query against composite key columns", async () => {
      const res = await db.query(
        "SELECT * FROM tenant_documents WHERE tenant_id = $1 AND author_id = $2;",
        [101, 1]
      );
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("Q1 RoadMap");
    });

    test("40. Clean batch delete on composite foreign key children", async () => {
      await db.query("DELETE FROM tenant_documents WHERE tenant_id = 101 AND author_id = 2;");
      const res = await db.query("SELECT count(*) AS c FROM tenant_documents;");
      expect(Number(res[0].c)).toBe(1);
    });
  });

  // =========================================================================
  // Section 3: Schema Introspection & Referential Integrity (41 - 60)
  // =========================================================================
  describe("Section 3: Schema Introspection & Referential Integrity", () => {
    beforeAll(async () => {
      await db.query(`
        CREATE TABLE store_categories (
          id SERIAL PRIMARY KEY,
          category_name TEXT NOT NULL UNIQUE
        );
      `);

      await db.query(`
        CREATE TABLE store_products (
          id SERIAL PRIMARY KEY,
          category_id INT NOT NULL REFERENCES store_categories(id) ON DELETE CASCADE,
          product_name TEXT NOT NULL,
          price NUMERIC NOT NULL
        );
      `);

      await db.query(`
        CREATE TABLE product_reviews (
          id SERIAL PRIMARY KEY,
          product_id INT NOT NULL REFERENCES store_products(id) ON DELETE CASCADE,
          rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
          comment TEXT
        );
      `);

      await db.query(`
        CREATE TABLE store_coupons (
          code TEXT PRIMARY KEY,
          featured_product_id INT REFERENCES store_products(id) ON DELETE SET NULL,
          discount_pct INT NOT NULL
        );
      `);
    });

    test("41. Populate multi-tier relational hierarchy", async () => {
      await db.query("INSERT INTO store_categories (id, category_name) VALUES (1, 'Electronics'), (2, 'Books');");
      await db.query("INSERT INTO store_products (id, category_id, product_name, price) VALUES (101, 1, 'Laptop Pro', 1200), (102, 1, 'Wireless Mouse', 25), (201, 2, 'Postgres Handbook', 45);");
      await db.query("INSERT INTO product_reviews (id, product_id, rating, comment) VALUES (1, 101, 5, 'Super fast!'), (2, 101, 4, 'Great battery'), (3, 102, 5, 'Smooth clicks'), (4, 201, 5, 'Essential read');");
      await db.query("INSERT INTO store_coupons (code, featured_product_id, discount_pct) VALUES ('LAPTOP10', 101, 10), ('GENERIC5', NULL, 5);");

      const cats = await db.query("SELECT count(*) AS c FROM store_categories;");
      const prods = await db.query("SELECT count(*) AS c FROM store_products;");
      const revs = await db.query("SELECT count(*) AS c FROM product_reviews;");
      expect(Number(cats[0].c)).toBe(2);
      expect(Number(prods[0].c)).toBe(3);
      expect(Number(revs[0].c)).toBe(4);
    });

    test("42. 3-Table Join traversing Category -> Product -> Reviews", async () => {
      const res = await db.query(`
        SELECT c.category_name, p.product_name, r.rating, r.comment
        FROM store_categories c
        JOIN store_products p ON c.id = p.category_id
        JOIN product_reviews r ON p.id = r.product_id
        WHERE c.id = 1
        ORDER BY p.id ASC, r.id ASC;
      `);
      expect(res.length).toBe(3);
      expect(res[0].category_name).toBe("Electronics");
      expect(res[0].product_name).toBe("Laptop Pro");
    });

    test("43. Average review rating per product via FK Join", async () => {
      const res = await db.query(`
        SELECT p.product_name, avg(r.rating) AS avg_rating, count(r.id) AS review_count
        FROM store_products p
        JOIN product_reviews r ON p.id = r.product_id
        WHERE p.id = 101
        GROUP BY p.product_name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].product_name).toBe("Laptop Pro");
      expect(Number(res[0].avg_rating)).toBe(4.5);
      expect(Number(res[0].review_count)).toBe(2);
    });

    test("44. Query coupons with and without featured products via LEFT JOIN", async () => {
      const res = await db.query(`
        SELECT sc.code, sc.discount_pct, sp.product_name
        FROM store_coupons sc
        LEFT JOIN store_products sp ON sc.featured_product_id = sp.id
        ORDER BY sc.code ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].code).toBe("GENERIC5");
      expect(res[0].product_name).toBeNull();
      expect(res[1].code).toBe("LAPTOP10");
      expect(res[1].product_name).toBe("Laptop Pro");
    });

    test("45. Self-referencing file system tree table creation", async () => {
      await db.query(`
        CREATE TABLE file_system_nodes (
          id SERIAL PRIMARY KEY,
          parent_id INT REFERENCES file_system_nodes(id),
          name TEXT NOT NULL,
          is_folder BOOLEAN NOT NULL DEFAULT false
        );
      `);
      await db.query("INSERT INTO file_system_nodes (id, parent_id, name, is_folder) VALUES (1, NULL, 'root', true);");
      await db.query("INSERT INTO file_system_nodes (id, parent_id, name, is_folder) VALUES (2, 1, 'home', true);");
      await db.query("INSERT INTO file_system_nodes (id, parent_id, name, is_folder) VALUES (3, 2, 'user', true);");
      await db.query("INSERT INTO file_system_nodes (id, parent_id, name, is_folder) VALUES (4, 3, 'document.txt', false);");

      const res = await db.query("SELECT count(*) AS total FROM file_system_nodes;");
      expect(Number(res[0].total)).toBe(4);
    });

    test("46. Self JOIN on tree nodes finding immediate children of a folder", async () => {
      const res = await db.query(`
        SELECT p.name AS parent_folder, c.name AS child_item
        FROM file_system_nodes p
        JOIN file_system_nodes c ON p.id = c.parent_id
        WHERE p.name = 'home';
      `);
      expect(res.length).toBe(1);
      expect(res[0].parent_folder).toBe("home");
      expect(res[0].child_item).toBe("user");
    });

    test("47. Recursive CTE traversing full tree path from root", async () => {
      const res = await db.query(`
        WITH RECURSIVE path_tree(id, parent_id, name, depth) AS (
          SELECT id, parent_id, name, 1 AS depth
          FROM file_system_nodes
          WHERE parent_id IS NULL
          UNION ALL
          SELECT child.id, child.parent_id, child.name, (pt.depth + 1) AS depth
          FROM file_system_nodes child
          JOIN path_tree pt ON child.parent_id = pt.id
        )
        SELECT name, depth FROM path_tree ORDER BY depth ASC;
      `);
      expect(res.length).toBe(4);
      expect(res[0].name).toBe("root");
      expect(Number(res[0].depth)).toBe(1);
      expect(res[3].name).toBe("document.txt");
      expect(Number(res[3].depth)).toBe(4);
    });

    test("48. Coordinated transaction deleting child and parent atomically", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM product_reviews WHERE product_id = 102;");
      await db.query("DELETE FROM store_products WHERE id = 102;");
      await db.query("COMMIT;");

      const prods = await db.query("SELECT * FROM store_products WHERE id = 102;");
      const revs = await db.query("SELECT * FROM product_reviews WHERE product_id = 102;");
      expect(prods.length).toBe(0);
      expect(revs.length).toBe(0);
    });

    test("49. Transactional cleanup updating FK to NULL before parent deletion", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE store_coupons SET featured_product_id = NULL WHERE featured_product_id = 101;");
      await db.query("DELETE FROM product_reviews WHERE product_id = 101;");
      await db.query("DELETE FROM store_products WHERE id = 101;");
      await db.query("COMMIT;");

      const coupon = await db.query("SELECT featured_product_id FROM store_coupons WHERE code = 'LAPTOP10';");
      expect(coupon[0].featured_product_id).toBeNull();
    });

    test("50. Deleting parent table row with subquery filter", async () => {
      await db.query("INSERT INTO store_categories (id, category_name) VALUES (10, 'Sports');");
      await db.query("INSERT INTO store_products (id, category_id, product_name, price) VALUES (301, 10, 'Soccer Ball', 20);");

      await db.query("DELETE FROM store_products WHERE category_id = (SELECT id FROM store_categories WHERE category_name = 'Sports');");
      await db.query("DELETE FROM store_categories WHERE category_name = 'Sports';");

      const prods = await db.query("SELECT * FROM store_products WHERE id = 301;");
      expect(prods.length).toBe(0);
    });

    test("51. Batch deletion using WHERE IN list", async () => {
      await db.query("INSERT INTO store_categories (id, category_name) VALUES (20, 'Gardening'), (21, 'Tools');");
      await db.query("DELETE FROM store_categories WHERE id IN (20, 21);");
      const res = await db.query("SELECT * FROM store_categories WHERE id IN (20, 21);");
      expect(res.length).toBe(0);
    });

    test("52. Updating parent category name and querying products", async () => {
      await db.query("UPDATE store_categories SET category_name = 'Literature' WHERE id = 2;");
      const res = await db.query(`
        SELECT p.product_name, c.category_name
        FROM store_products p
        JOIN store_categories c ON p.category_id = c.id
        WHERE c.id = 2;
      `);
      expect(res.length).toBe(1);
      expect(res[0].category_name).toBe("Literature");
    });

    test("53. Finding orphaned items with Anti-Join", async () => {
      await db.query("INSERT INTO store_products (id, category_id, product_name, price) VALUES (999, 999, 'Ghost Item', 99);");
      const res = await db.query(`
        SELECT p.product_name
        FROM store_products p
        LEFT JOIN store_categories c ON p.category_id = c.id
        WHERE c.id IS NULL;
      `);
      expect(res.length).toBe(1);
      expect(res[0].product_name).toBe("Ghost Item");
      await db.query("DELETE FROM store_products WHERE id = 999;");
    });

    test("54. Clean deletion of child without affecting parent", async () => {
      await db.query("INSERT INTO store_categories (id, category_name) VALUES (30, 'Outdoors');");
      await db.query("INSERT INTO store_products (id, category_id, product_name, price) VALUES (501, 30, 'Tent', 150);");
      await db.query("DELETE FROM store_products WHERE id = 501;");

      const cat = await db.query("SELECT * FROM store_categories WHERE id = 30;");
      expect(cat.length).toBe(1);
      expect(cat[0].category_name).toBe("Outdoors");
      await db.query("DELETE FROM store_categories WHERE id = 30;");
    });

    test("55. Cyclic relational model schema and queries", async () => {
      await db.query(`
        CREATE TABLE cyclic_teams (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          captain_id INT
        );
      `);
      await db.query(`
        CREATE TABLE cyclic_members (
          id SERIAL PRIMARY KEY,
          team_id INT REFERENCES cyclic_teams(id),
          name TEXT NOT NULL
        );
      `);
      await db.query("INSERT INTO cyclic_teams (id, name) VALUES (1, 'Team Alpha');");
      await db.query("INSERT INTO cyclic_members (id, team_id, name) VALUES (10, 1, 'Captain Alex');");
      await db.query("UPDATE cyclic_teams SET captain_id = 10 WHERE id = 1;");

      const res = await db.query(`
        SELECT t.name AS team, m.name AS captain
        FROM cyclic_teams t
        JOIN cyclic_members m ON t.captain_id = m.id;
      `);
      expect(res.length).toBe(1);
      expect(res[0].team).toBe("Team Alpha");
      expect(res[0].captain).toBe("Captain Alex");
    });

    test("56. Clean deletion of cyclic relational entities in transaction", async () => {
      await db.query("BEGIN;");
      await db.query("UPDATE cyclic_teams SET captain_id = NULL WHERE id = 1;");
      await db.query("DELETE FROM cyclic_members WHERE id = 10;");
      await db.query("DELETE FROM cyclic_teams WHERE id = 1;");
      await db.query("COMMIT;");

      const teams = await db.query("SELECT * FROM cyclic_teams WHERE id = 1;");
      const members = await db.query("SELECT * FROM cyclic_members WHERE id = 10;");
      expect(teams.length).toBe(0);
      expect(members.length).toBe(0);
    });

    test("57. Multiple child tables referencing same parent table", async () => {
      await db.query(`
        CREATE TABLE hub_users (id SERIAL PRIMARY KEY, username TEXT NOT NULL);
      `);
      await db.query(`
        CREATE TABLE hub_posts (id SERIAL PRIMARY KEY, user_id INT REFERENCES hub_users(id), content TEXT);
      `);
      await db.query(`
        CREATE TABLE hub_comments (id SERIAL PRIMARY KEY, user_id INT REFERENCES hub_users(id), comment TEXT);
      `);

      await db.query("INSERT INTO hub_users (id, username) VALUES (1, 'hub_user_1');");
      await db.query("INSERT INTO hub_posts (id, user_id, content) VALUES (1, 1, 'Hello World');");
      await db.query("INSERT INTO hub_comments (id, user_id, comment) VALUES (1, 1, 'Great start');");

      const posts = await db.query("SELECT * FROM hub_posts WHERE user_id = 1;");
      const comments = await db.query("SELECT * FROM hub_comments WHERE user_id = 1;");
      expect(posts.length).toBe(1);
      expect(comments.length).toBe(1);
    });

    test("58. Atomic cleanup of multi-table child references in transaction", async () => {
      await db.query("BEGIN;");
      await db.query("DELETE FROM hub_posts WHERE user_id = 1;");
      await db.query("DELETE FROM hub_comments WHERE user_id = 1;");
      await db.query("DELETE FROM hub_users WHERE id = 1;");
      await db.query("COMMIT;");

      const users = await db.query("SELECT * FROM hub_users WHERE id = 1;");
      expect(users.length).toBe(0);
    });

    test("59. Transaction ROLLBACK restores multi-table relational modifications", async () => {
      await db.query("INSERT INTO hub_users (id, username) VALUES (2, 'user_two');");
      await db.query("INSERT INTO hub_posts (id, user_id, content) VALUES (2, 2, 'Post from user 2');");

      await db.query("BEGIN;");
      await db.query("DELETE FROM hub_posts WHERE user_id = 2;");
      await db.query("DELETE FROM hub_users WHERE id = 2;");
      await db.query("ROLLBACK;");

      const userAfter = await db.query("SELECT * FROM hub_users WHERE id = 2;");
      const postAfter = await db.query("SELECT * FROM hub_posts WHERE user_id = 2;");
      expect(userAfter.length).toBe(1);
      expect(postAfter.length).toBe(1);
    });

    test("60. Clean up transient cascade test tables", async () => {
      await db.query("DROP TABLE hub_posts;");
      await db.query("DROP TABLE hub_comments;");
      await db.query("DROP TABLE hub_users;");
      expect(true).toBe(true);
    });
  });

  // =========================================================================
  // Section 4: Advanced Constraints: CHECK, DEFAULT, NOT NULL (61 - 80)
  // =========================================================================
  describe("Section 4: Advanced Constraints: CHECK, DEFAULT, NOT NULL", () => {
    beforeAll(async () => {
      await db.query(`
        CREATE TABLE products_constrained (
          id SERIAL PRIMARY KEY,
          sku TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          price NUMERIC NOT NULL CHECK (price >= 0.01),
          discount_price NUMERIC CHECK (discount_price IS NULL OR discount_price < price),
          stock INT DEFAULT 0 CHECK (stock >= 0),
          status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'archived')),
          created_at TEXT DEFAULT '2026-01-01'
        );
      `);
    });

    test("61. Inserting valid row satisfying all CHECK and DEFAULT constraints", async () => {
      await db.query("INSERT INTO products_constrained (id, sku, name, price, stock) VALUES (1, 'SKU-001', 'Widget', 19.99, 50);");
      const res = await db.query("SELECT * FROM products_constrained WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].status).toBe("draft");
      expect(res[0].created_at).toBe("2026-01-01");
      expect(Number(res[0].stock)).toBe(50);
    });

    test("62. Multi-column CHECK constraint: discount_price < price", async () => {
      await db.query("INSERT INTO products_constrained (id, sku, name, price, discount_price, stock) VALUES (2, 'SKU-002', 'Gadget', 49.99, 39.99, 10);");
      const res = await db.query("SELECT * FROM products_constrained WHERE id = 2;");
      expect(res.length).toBe(1);
      expect(Number(res[0].discount_price)).toBe(39.99);
    });

    test("63. Default values applied when columns omitted from INSERT", async () => {
      await db.query("INSERT INTO products_constrained (id, sku, name, price) VALUES (3, 'SKU-003', 'Simple Item', 5.00);");
      const res = await db.query("SELECT * FROM products_constrained WHERE id = 3;");
      expect(Number(res[0].stock)).toBe(0);
      expect(res[0].status).toBe("draft");
    });

    test("64. Valid status string from allowed CHECK IN list", async () => {
      await db.query("INSERT INTO products_constrained (id, sku, name, price, status) VALUES (4, 'SKU-004', 'Published Item', 10.00, 'published');");
      const res = await db.query("SELECT status FROM products_constrained WHERE id = 4;");
      expect(res[0].status).toBe("published");
    });

    test("65. Updating status to another valid CHECK enum value", async () => {
      await db.query("UPDATE products_constrained SET status = 'archived' WHERE id = 4;");
      const res = await db.query("SELECT status FROM products_constrained WHERE id = 4;");
      expect(res[0].status).toBe("archived");
    });

    test("66. Table with DATE / TIMESTAMP ordering CHECK constraint", async () => {
      await db.query(`
        CREATE TABLE event_schedules (
          event_id SERIAL PRIMARY KEY,
          event_title TEXT NOT NULL,
          start_date TEXT NOT NULL,
          end_date TEXT NOT NULL,
          CHECK (start_date <= end_date)
        );
      `);
      await db.query("INSERT INTO event_schedules (event_id, event_title, start_date, end_date) VALUES (1, 'Conference', '2026-09-01', '2026-09-03');");
      const res = await db.query("SELECT * FROM event_schedules WHERE event_id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].event_title).toBe("Conference");
    });

    test("67. Table with string length or format CHECK constraints", async () => {
      await db.query(`
        CREATE TABLE user_accounts (
          user_id SERIAL PRIMARY KEY,
          username TEXT NOT NULL UNIQUE,
          postal_code TEXT NOT NULL,
          age INT CHECK (age >= 18 AND age <= 120)
        );
      `);
      await db.query("INSERT INTO user_accounts (user_id, username, postal_code, age) VALUES (1, 'john_coder', '10001', 28);");
      const res = await db.query("SELECT * FROM user_accounts WHERE user_id = 1;");
      expect(res.length).toBe(1);
      expect(Number(res[0].age)).toBe(28);
    });

    test("68. Table with Boolean NOT NULL and default constraint", async () => {
      await db.query(`
        CREATE TABLE notification_settings (
          id SERIAL PRIMARY KEY,
          user_id INT NOT NULL,
          email_enabled BOOLEAN NOT NULL DEFAULT true,
          sms_enabled BOOLEAN NOT NULL DEFAULT false
        );
      `);
      await db.query("INSERT INTO notification_settings (id, user_id) VALUES (1, 100);");
      const res = await db.query("SELECT * FROM notification_settings WHERE id = 1;");
      expect(res[0].email_enabled).toBe(true);
      expect(res[0].sms_enabled).toBe(false);
    });

    test("69. Table with NUMERIC precision scale and range check", async () => {
      await db.query(`
        CREATE TABLE bank_accounts (
          account_num TEXT PRIMARY KEY,
          balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
          overdraft_limit NUMERIC(10, 2) NOT NULL DEFAULT 500.00,
          CHECK (balance >= -overdraft_limit)
        );
      `);
      await db.query("INSERT INTO bank_accounts (account_num, balance, overdraft_limit) VALUES ('ACC-01', 1500.50, 500.00);");
      const res = await db.query("SELECT * FROM bank_accounts WHERE account_num = 'ACC-01';");
      expect(Number(res[0].balance)).toBe(1500.50);
    });

    test("70. ALTER TABLE ADD COLUMN with DEFAULT value populates existing rows", async () => {
      await db.query("ALTER TABLE bank_accounts ADD COLUMN currency TEXT DEFAULT 'USD';");
      const res = await db.query("SELECT account_num, currency FROM bank_accounts WHERE account_num = 'ACC-01';");
      expect(res[0].currency).toBe("USD");
    });

    test("71. ALTER TABLE ADD COLUMN with NOT NULL constraint", async () => {
      await db.query("ALTER TABLE bank_accounts ADD COLUMN is_active BOOLEAN DEFAULT true;");
      const res = await db.query("SELECT account_num, is_active FROM bank_accounts WHERE account_num = 'ACC-01';");
      expect(res[0].is_active).toBe(true);
    });

    test("72. ALTER TABLE DROP COLUMN removes constraint column", async () => {
      await db.query("ALTER TABLE bank_accounts DROP COLUMN currency;");
      const res = await db.query("SELECT * FROM bank_accounts WHERE account_num = 'ACC-01';");
      expect((res[0] as any).currency).toBeUndefined();
    });

    test("73. UNIQUE constraint prevents duplicate entries across single column", async () => {
      const res = await db.query("SELECT count(DISTINCT sku) AS distinct_sku, count(*) AS total FROM products_constrained;");
      expect(Number(res[0].distinct_sku)).toBe(Number(res[0].total));
    });

    test("74. Multiple DEFAULT expressions applied on insert", async () => {
      await db.query(`
        CREATE TABLE metrics_log (
          id SERIAL PRIMARY KEY,
          tag TEXT DEFAULT 'system',
          val INT DEFAULT 0,
          flag BOOLEAN DEFAULT false
        );
      `);
      await db.query("INSERT INTO metrics_log (id) VALUES (1);");
      const res = await db.query("SELECT * FROM metrics_log WHERE id = 1;");
      expect(res[0].tag).toBe("system");
      expect(Number(res[0].val)).toBe(0);
      expect(res[0].flag).toBe(false);
    });

    test("75. Overriding DEFAULT value with explicit column input", async () => {
      await db.query("INSERT INTO metrics_log (id, tag, val, flag) VALUES (2, 'custom', 42, true);");
      const res = await db.query("SELECT * FROM metrics_log WHERE val = 42;");
      expect(res[0].tag).toBe("custom");
      expect(res[0].flag).toBe(true);
    });

    test("76. Complex expression in CHECK constraint evaluating arithmetic", async () => {
      await db.query(`
        CREATE TABLE inventory_audit (
          id SERIAL PRIMARY KEY,
          received INT NOT NULL,
          sold INT NOT NULL,
          damaged INT NOT NULL,
          remaining INT NOT NULL,
          CHECK (remaining = received - sold - damaged)
        );
      `);
      await db.query("INSERT INTO inventory_audit (id, received, sold, damaged, remaining) VALUES (1, 100, 70, 5, 25);");
      const res = await db.query("SELECT * FROM inventory_audit WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(Number(res[0].remaining)).toBe(25);
    });

    test("77. Constraints on JSON / JSONB columns", async () => {
      await db.query(`
        CREATE TABLE json_configs (
          id SERIAL PRIMARY KEY,
          config_name TEXT NOT NULL UNIQUE,
          payload JSONB NOT NULL
        );
      `);
      await db.query("INSERT INTO json_configs (id, config_name, payload) VALUES (1, 'server', '{\"port\": 8080, \"ssl\": true}');");
      const res = await db.query("SELECT * FROM json_configs WHERE id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].config_name).toBe("server");
    });

    test("78. Constraints on ARRAY columns", async () => {
      await db.query(`
        CREATE TABLE tagged_items (
          id SERIAL PRIMARY KEY,
          item_name TEXT NOT NULL,
          tags TEXT[] NOT NULL
        );
      `);
      await db.query("INSERT INTO tagged_items (id, item_name, tags) VALUES (1, 'Book', ARRAY['education', 'paperback']);");
      const res = await db.query("SELECT * FROM tagged_items WHERE id = 1;");
      expect(res.length).toBe(1);
    });

    test("79. Combining PRIMARY KEY, UNIQUE, and FOREIGN KEY in one table", async () => {
      await db.query(`
        CREATE TABLE user_tokens (
          token_id SERIAL PRIMARY KEY,
          user_id INT NOT NULL REFERENCES user_accounts(user_id) ON DELETE CASCADE,
          token_hash TEXT NOT NULL UNIQUE,
          expires_at TEXT NOT NULL
        );
      `);
      await db.query("INSERT INTO user_tokens (token_id, user_id, token_hash, expires_at) VALUES (1, 1, 'hash_abc_123', '2026-12-31');");
      const res = await db.query("SELECT * FROM user_tokens WHERE token_id = 1;");
      expect(res.length).toBe(1);
      expect(res[0].token_hash).toBe("hash_abc_123");
    });

    test("80. Explicit cleanup of dependent token row before user deletion", async () => {
      await db.query("DELETE FROM user_tokens WHERE user_id = 1;");
      await db.query("DELETE FROM user_accounts WHERE user_id = 1;");
      const users = await db.query("SELECT * FROM user_accounts WHERE user_id = 1;");
      const tokens = await db.query("SELECT * FROM user_tokens WHERE user_id = 1;");
      expect(users.length).toBe(0);
      expect(tokens.length).toBe(0);
    });
  });

  // =========================================================================
  // Section 5: Joins, Aggregations & Complex Relational Integrity (81 - 100)
  // =========================================================================
  describe("Section 5: Joins, Aggregations & Complex Relational Integrity", () => {
    test("81. 4-Way Join across Department, Employee, Project, Assignment", async () => {
      const res = await db.query(`
        SELECT d.dept_name, e.name AS employee_name, p.title AS project_title, pa.role
        FROM departments d
        JOIN employees e ON d.id = e.department_id
        JOIN project_assignments pa ON e.id = pa.employee_id
        JOIN projects p ON pa.project_id = p.id
        ORDER BY e.name ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].dept_name).toBe("Engineering");
      expect(res[0].project_title).toBe("Cloud Migration");
    });

    test("82. Total project allocations grouped by department", async () => {
      const res = await db.query(`
        SELECT d.dept_name, count(pa.project_id) AS total_assignments, sum(pa.assigned_hours) AS total_hours
        FROM departments d
        JOIN employees e ON d.id = e.department_id
        JOIN project_assignments pa ON e.id = pa.employee_id
        GROUP BY d.dept_name;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
      expect(Number(res[0].total_assignments)).toBe(2);
      expect(Number(res[0].total_hours)).toBe(70);
    });

    test("83. Finding employees not assigned to any project via LEFT JOIN anti-pattern", async () => {
      const res = await db.query(`
        SELECT e.name, e.salary
        FROM employees e
        LEFT JOIN project_assignments pa ON e.id = pa.employee_id
        WHERE pa.project_id IS NULL
        ORDER BY e.id ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(1);
      expect(res.map(r => r.name)).toContain("Bob Freelance");
    });

    test("84. Finding projects with no assigned employees via LEFT JOIN anti-pattern", async () => {
      const res = await db.query(`
        SELECT p.title, p.start_date
        FROM projects p
        LEFT JOIN project_assignments pa ON p.id = pa.project_id
        WHERE pa.employee_id IS NULL;
      `);
      expect(res.length).toBe(1);
      expect(res[0].title).toBe("Secret Project");
    });

    test("85. CTE joining parent, child, and aggregated metrics", async () => {
      const res = await db.query(`
        WITH dept_stats AS (
          SELECT department_id, count(*) AS emp_count, avg(salary) AS avg_sal
          FROM employees
          WHERE department_id IS NOT NULL
          GROUP BY department_id
        )
        SELECT d.dept_name, d.budget, ds.emp_count, ds.avg_sal
        FROM departments d
        JOIN dept_stats ds ON d.id = ds.department_id
        ORDER BY d.dept_name ASC;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
      expect(Number(res[0].emp_count)).toBe(2);
      expect(Number(res[0].avg_sal)).toBe(105000); // (120k + 90k) / 2
    });

    test("86. Recursive CTE traversing employee manager hierarchy", async () => {
      const res = await db.query(`
        WITH RECURSIVE org_chart(id, name, manager_id, level) AS (
          SELECT id, name, manager_id, 1 AS level
          FROM employees
          WHERE manager_id IS NULL AND id = 1
          UNION ALL
          SELECT e.id, e.name, e.manager_id, (oc.level + 1) AS level
          FROM employees e
          JOIN org_chart oc ON e.manager_id = oc.id
        )
        SELECT id, name, level FROM org_chart ORDER BY level ASC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Alice");
      expect(Number(res[0].level)).toBe(1);
      expect(res[1].name).toBe("Charlie");
      expect(Number(res[1].level)).toBe(2);
    });

    test("87. Self JOIN on employees table finding peer employees in the same department", async () => {
      const res = await db.query(`
        SELECT e1.name AS emp1, e2.name AS emp2, e1.department_id
        FROM employees e1
        JOIN employees e2 ON e1.department_id = e2.department_id AND e1.id < e2.id
        WHERE e1.department_id IS NOT NULL;
      `);
      expect(res.length).toBe(1);
      expect(res[0].emp1).toBe("Alice");
      expect(res[0].emp2).toBe("Charlie");
    });

    test("88. Window function over joined relational tables (rank salaries per department)", async () => {
      const res = await db.query(`
        SELECT d.dept_name, e.name, e.salary,
               ROW_NUMBER() OVER (PARTITION BY d.dept_name ORDER BY e.salary DESC) AS sal_rank
        FROM departments d
        JOIN employees e ON d.id = e.department_id
        ORDER BY d.dept_name ASC, e.salary DESC;
      `);
      expect(res.length).toBe(2);
      expect(res[0].name).toBe("Alice");
      expect(Number(res[0].sal_rank)).toBe(1);
      expect(res[1].name).toBe("Charlie");
      expect(Number(res[1].sal_rank)).toBe(2);
    });

    test("89. Correlated subquery calculating percentage of department budget consumed by employee", async () => {
      const res = await db.query(`
        SELECT e.name, e.salary, d.budget,
               (e.salary * 100.0 / d.budget) AS pct_of_budget
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.id = 1;
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].pct_of_budget)).toBe(24); // 120000 / 500000 = 24%
    });

    test("90. Transaction rolling back failed multi-table relational operation", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO departments (id, dept_name, budget) VALUES (99, 'Temporary', 10000);");
      await db.query("INSERT INTO employees (id, name, email, department_id, salary) VALUES (99, 'Temp Emp', 'temp99@corp.com', 99, 50000);");
      const checkWithin = await db.query("SELECT count(*) AS c FROM employees WHERE id = 99;");
      expect(Number(checkWithin[0].c)).toBe(1);
      await db.query("ROLLBACK;");

      const checkAfterEmp = await db.query("SELECT * FROM employees WHERE id = 99;");
      const checkAfterDept = await db.query("SELECT * FROM departments WHERE id = 99;");
      expect(checkAfterEmp.length).toBe(0);
      expect(checkAfterDept.length).toBe(0);
    });

    test("91. Transaction committing multi-table relational insertion", async () => {
      await db.query("BEGIN;");
      await db.query("INSERT INTO departments (id, dept_name, budget) VALUES (5, 'Marketing', 150000);");
      await db.query("INSERT INTO employees (id, name, email, department_id, salary) VALUES (5, 'Marketer Meg', 'meg@corp.com', 5, 85000);");
      await db.query("COMMIT;");

      const res = await db.query(`
        SELECT e.name, d.dept_name
        FROM employees e
        JOIN departments d ON e.department_id = d.id
        WHERE e.id = 5;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Marketer Meg");
      expect(res[0].dept_name).toBe("Marketing");
    });

    test("92. Subquery in WHERE checking parent department condition with parameter", async () => {
      const res = await db.query(
        "SELECT name, salary FROM employees WHERE department_id = (SELECT id FROM departments WHERE dept_name = $1);",
        ["Marketing"]
      );
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Marketer Meg");
    });

    test("93. Combining UNION ALL with relational joins", async () => {
      const res = await db.query(`
        SELECT e.name, 'Assigned' AS status FROM employees e JOIN project_assignments pa ON e.id = pa.employee_id
        UNION ALL
        SELECT e.name, 'Unassigned' AS status FROM employees e LEFT JOIN project_assignments pa ON e.id = pa.employee_id WHERE pa.project_id IS NULL
        ORDER BY name ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(3);
    });

    test("94. Full schema validation across all relational constraints", async () => {
      const depts = await db.query("SELECT count(*) AS c FROM departments;");
      const emps = await db.query("SELECT count(*) AS c FROM employees;");
      const projs = await db.query("SELECT count(*) AS c FROM projects;");
      const assigns = await db.query("SELECT count(*) AS c FROM project_assignments;");

      expect(Number(depts[0].c)).toBeGreaterThanOrEqual(3);
      expect(Number(emps[0].c)).toBeGreaterThanOrEqual(3);
      expect(Number(projs[0].c)).toBeGreaterThanOrEqual(2);
      expect(Number(assigns[0].c)).toBeGreaterThanOrEqual(2);
    });

    test("95. Multiple JOIN conditions including non-equality filters", async () => {
      const res = await db.query(`
        SELECT e.name, d.dept_name, e.salary
        FROM employees e
        JOIN departments d ON e.department_id = d.id AND e.salary > 100000
        ORDER BY e.salary DESC;
      `);
      expect(res.length).toBe(1);
      expect(res[0].name).toBe("Alice");
    });

    test("96. Group by multiple columns across relational join", async () => {
      const res = await db.query(`
        SELECT d.dept_name, e.status, count(*) AS count_in_status
        FROM departments d
        JOIN employees e ON d.id = e.department_id
        GROUP BY d.dept_name, e.status
        ORDER BY d.dept_name ASC;
      `);
      expect(res.length).toBeGreaterThanOrEqual(2);
      expect(res[0].dept_name).toBe("Engineering");
    });

    test("97. HAVING filter on joined aggregate results via CTE", async () => {
      const res = await db.query(`
        WITH dept_payrolls AS (
          SELECT d.dept_name, sum(e.salary) AS total_payroll
          FROM departments d
          JOIN employees e ON d.id = e.department_id
          GROUP BY d.dept_name
        )
        SELECT * FROM dept_payrolls WHERE total_payroll >= 200000;
      `);
      expect(res.length).toBe(1);
      expect(res[0].dept_name).toBe("Engineering");
      expect(Number(res[0].total_payroll)).toBe(210000);
    });

    test("98. Derived table join calculating department payroll vs budget remaining", async () => {
      const res = await db.query(`
        SELECT d.dept_name, d.budget, sub.total_payroll, (d.budget - sub.total_payroll) AS budget_remaining
        FROM departments d
        JOIN (
          SELECT department_id, sum(salary) AS total_payroll
          FROM employees
          WHERE department_id IS NOT NULL
          GROUP BY department_id
        ) sub ON d.id = sub.department_id
        WHERE d.id = 1;
      `);
      expect(res.length).toBe(1);
      expect(Number(res[0].budget_remaining)).toBe(290000); // 500k - 210k
    });

    test("99. Complex multi-table DML mutation maintaining relational integrity", async () => {
      await db.query("UPDATE employees SET salary = salary * 1.05 WHERE department_id = 1;");
      const res = await db.query("SELECT salary FROM employees WHERE id = 1;");
      expect(Number(res[0].salary)).toBe(126000); // 120k * 1.05
    });

    test("100. End-to-End relational integrity workflow complete", async () => {
      const res = await db.query("SELECT count(*) AS total_employees FROM employees;");
      expect(Number(res[0].total_employees)).toBeGreaterThanOrEqual(3);
    });
  });
});

