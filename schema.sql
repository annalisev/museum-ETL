-- This file should contain all code required to create & seed database tables.

DROP DATABASE museum;
CREATE DATABASE museum;
\c museum

CREATE TABLE floor (
    floor_id INT GENERATED ALWAYS AS IDENTITY,
    floor_name VARCHAR(20) UNIQUE NOT NULL,
    PRIMARY KEY (floor_id)
);


CREATE TABLE department (
    department_id INT GENERATED ALWAYS AS IDENTITY,
    department_name VARCHAR(30) UNIQUE NOT NULL,
    PRIMARY KEY (department_id)
);

CREATE TABLE exhibition (
    exhibition_id INT GENERATED ALWAYS AS IDENTITY,
    exhibition_name VARCHAR(30) UNIQUE NOT NULL,
    floor_id INT NOT NULL,
    starting_date DATE NOT NULL,
    summary TEXT,
    department_id INT,
    PRIMARY KEY (exhibition_id),
    FOREIGN KEY(floor_id) REFERENCES floor(floor_id),
    FOREIGN KEY(department_id) REFERENCES department(department_id)
);


CREATE TABLE rating_value (
    rating_value_id INT GENERATED ALWAYS AS IDENTITY,
    rating_number INT UNIQUE NOT NULL,
    phrase VARCHAR(20) UNIQUE,
    PRIMARY KEY (rating_value_id)
);

CREATE TABLE rating (
    rating_id INT GENERATED ALWAYS AS IDENTITY,
    time_given TIMESTAMP NOT NULL CHECK(time_given <= NOW()),
    rating_value_id INT NOT NULL,
    exhibition_id INT NOT NULL,
    PRIMARY KEY (rating_id),
    FOREIGN KEY(rating_value_id) REFERENCES rating_value(rating_value_id),
    FOREIGN KEY(exhibition_id) REFERENCES exhibition(exhibition_id)
);

CREATE TABLE action_type (
    action_type_id INT GENERATED ALWAYS AS IDENTITY,
    action_name VARCHAR(20) UNIQUE NOT NULL,
    PRIMARY KEY (action_type_id)
);

CREATE TABLE action (
    action_id INT GENERATED ALWAYS AS IDENTITY,
    exhibition_id INT NOT NULL,
    time_occurred TIMESTAMP NOT NULL CHECK(time_occurred <= NOW()),
    action_type_id INT NOT NULL,
    PRIMARY KEY (action_id),
    FOREIGN KEY(action_type_id) REFERENCES action_type(action_type_id),
    FOREIGN KEY(exhibition_id) REFERENCES exhibition(exhibition_id)
);




INSERT INTO floor
    (floor_name)
VALUES
    ('Vault'),
    ('Floor 1'),
    ('Floor 2'),
    ('Floor 3'),
    ('Planetarium');


INSERT INTO department
    (department_name)
VALUES
    ('Entomology'),
    ('Geology'),
    ('Paleontology'),
    ('Ecology'),
    ('Zoology');


INSERT INTO rating_value
    (rating_number, phrase)
VALUES
    (0, 'Terrible'),
    (1, 'Bad'),
    (2, 'Neutral'),
    (3, 'Good'),
    (4, 'Amazing');


INSERT INTO action_type
    (action_name)
VALUES
    ('assistance'),
    ('emergency');



INSERT INTO exhibition
    (exhibition_name, floor_id, starting_date, summary, department_id)
VALUES
    ('Measureless to Man', 2, date '2021-08-23',
    'An immersive 3D experience: delve deep into a previously-inaccessible cave system.', 2 ),
    
    ('Adaptation', 1, date '2019-07-01', 
    'How insect evolution has kept pace with an industrialised world', 1 ),

    ('The Crenshaw Collection', 3, date '2021-03-03',
    'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.', 5 ),
    
    ('Cetacean Sensations', 2, date '2019-07-01',
    'Whales: from ancient myth to critically endangered.', 5 ),

    ('Our Polluted World', 4, date '2021-05-12',
    'A hard-hitting exploration of humanity''s impact on the environment.', 4 ),

    ('Thunder Lizards', 2, date '2023-02-01',
    'How new research is making scientists rethink what dinosaurs really looked like.', 3 );
    
   