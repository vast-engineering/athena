CREATE KEYSPACE IF NOT EXISTS testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE testks;

CREATE TABLE users (
    id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    emails set<varchar>
);

INSERT INTO users (id, first_name, last_name, emails) VALUES(1234, 'Frodo', 'Baggins', {'f@baggins.com', 'baggins@gmail.com'});
INSERT INTO users (id, first_name, last_name, emails) VALUES(5678, 'Bilbo', 'Baggins', {'b@baggins.com', 'bilbo@gmail.com'});
INSERT INTO users (id, first_name, last_name, emails) VALUES(9876, 'Sauron', 'Melkorson', {'sauron@mordor.com'});
