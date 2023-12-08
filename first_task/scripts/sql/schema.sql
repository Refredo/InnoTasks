CREATE TABLE IF NOT EXISTS students(
    id SERIAL PRIMARY KEY,
    birthday TIMESTAMP,
    name TEXT,
    room INT,
    sex VARCHAR(1),
    FOREIGN KEY (room) REFERENCES rooms(id)
);

CREATE TABLE IF NOT EXISTS rooms(
    id SERIAL PRIMARY KEY,
    name VARCHAR(12)
);

CREATE INDEX IF NOT EXISTS students_index ON students(name);
CREATE INDEX IF NOT EXISTS rooms_index ON rooms(name);