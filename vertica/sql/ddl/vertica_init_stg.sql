CREATE TABLE chats
(
    msg_id       INT PRIMARY KEY,
    msg_time     TIMESTAMP,
    msg_from     INT,
    msg_to       INT,
    text_message VARCHAR(255),
    msg_group_id INT
);

CREATE TABLE groups
(
    id           INT PRIMARY KEY,
    owner_id     INT,
    group_name   VARCHAR(255),
    created_date TIMESTAMP
);

CREATE TABLE peoples
(
    id                INT PRIMARY KEY,
    name              VARCHAR(255),
    registration_date TIMESTAMP,
    country           VARCHAR(255),
    date_of_birthday  DATE,
    phone             VARCHAR(20),
    e_mail            VARCHAR(255)
);

