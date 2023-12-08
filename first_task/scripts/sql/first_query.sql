SELECT  rooms.name, COUNT(students.name) AS amount
FROM 
    students INNER JOIN rooms
    ON students.room = rooms.id
GROUP BY rooms.name