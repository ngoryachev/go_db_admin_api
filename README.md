Linux

  Run mysql DB via docker:

    `sudo docker run -p 3306:3306 -v $PWD:/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql`

  Setup mysql client:

    `sudo apt install mysql-client-core-8.0`

  To populate DB test data:

    `mysql --user=root --password=1234 --port=3306 --host=127.0.0.1 --protocol=tcp golang < sample_db.sql`  

  Connect via mysql client:

    `mysql --user=root --password=1234 --port=3306 --host=127.0.0.1 --protocol=tcp golang`

  Useful sql commands:
    
    `
    SHOW TABLES;
    SHOW FULL COLUMNS FROM `$table_name`; 
    SHOW KEYS FROM table WHERE Key_name = 'PRIMARY';
    INSERT INTO %s (%s) VALUES (%s)
    SELECT * FROM %s LIMIT %d OFFSET %d
    SELECT * FROM %s WHERE %s='%d'
    UPDATE items SET `title` = ?, `description` = ?, `updated` = ? WHERE id = ?
    DELETE FROM items WHERE id = ?"
    `




