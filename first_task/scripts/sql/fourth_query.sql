SELECT rooms.id, rooms.name
FROM
    rooms INNER JOIN students
    ON students.room = rooms.id
GROUP BY rooms.id
HAVING COUNT(DISTINCT sex) = 2