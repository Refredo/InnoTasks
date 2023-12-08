WITH students_age AS(
    SELECT 	id,  
            DATE_PART('year', NOW()) - DATE_PART('year', birthday)  AS age,
            name,
            room,
            sex
    FROM students
)

SELECT rooms.id, rooms.name
FROM
    rooms INNER JOIN students_age
    ON students_age.room = rooms.id
GROUP BY rooms.id
ORDER BY AVG(age) ASC
LIMIT 5