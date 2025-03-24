INSERT INTO customers (first_name, last_name, email, phone, address)
VALUES ('Yevhen', 'Yermolenko', 'email@e.com', '911', 'Vinnytsia')
RETURNING id, first_name, last_name, email;