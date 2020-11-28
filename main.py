import psycopg2 as psy
import names, time, random, pycountry

def connect_to_db(username, password, host = "127.0.0.1"):
    database = psy.connect("host='%s' dbname='schooldb' user='%s' password='%s'" % (host, username, password))
    cursor = database.cursor()

    return database, cursor

def create_tables(cursor):
    create_parents_query = """
    CREATE TABLE parents (
        parentid integer generated always as identity,
        name text not null,

        primary key(parentid)
    )

    """
    create_students_query = """
    CREATE TABLE students (
        studentid integer generated always as identity,
        name text not null,
        
        primary key(studentid)
    )
    """
    

    create_families_query = """
    CREATE TABLE families (
        id integer generated always as identity,
        studentid integer,
        parentid integer,

        primary key(id),

        constraint studentid 
        foreign key(studentid) references students(studentid),

        constraint parentid
        foreign key(parentid) references parents(parentid)
    )
    """

    create_teachers_query = """
    CREATE TABLE teachers (
        teacherid integer generated always as identity,
        name text not null,
        pay integer not null,

        primary key(teacherid)
    )
    """

    create_subjects_query = """
    CREATE TABLE subjects (
        subjectid integer generated always as identity,
        name text,
        teacherid integer,

        primary key(subjectid),

        constraint teacherid
        foreign key(teacherid) references teachers(teacherid)
    )
    """

    create_teacher_subjects_query = """
    CREATE TABLE teacher_subjects (
        id integer generated always as identity,
        subjectid integer,
        teacherid integer,

        primary key(id),

        constraint teacherid
        foreign key(teacherid) references teachers(teacherid),

        constraint subjectid
        foreign key(subjectid) references subjects(subjectid)
    )
    """

    create_grades_query = """
    CREATE TABLE grades (
        gradeid integer generated always as identity,
        grade integer not null,
        subjectid integer,
        studentid integer,

        primary key(gradeid),

        constraint subjectid
        foreign key(subjectid) references subjects(subjectid),

        constraint studentid
        foreign key(studentid) references students(studentid)
    )
    """


    create_missing_student_query = """
    CREATE TABLE missingstudents (
        missingstudentid integer generated always as identity,
        date timestamp,
        subjectid integer,
        studentid integer,

        primary key(missingstudentid),

        constraint subjectid
        foreign key(subjectid) references subjects(subjectid),

        constraint studentid
        foreign key(studentid) references students(studentid)
    )
    """

    create_substitutions_query = """
    CREATE TABLE substitutions (
        substitutionid integer generated always as identity,
        date timestamp not null,
        teacherid integer,
        substituteteacherid integer,
        subjectid integer,

        primary key(substitutionid),

        constraint teacherid
        foreign key(teacherid) references teachers(teacherid),
        
        constraint substituteteacherid
        foreign key(teacherid) references teachers(teacherid),

        constraint subjectid
        foreign key(subjectid) references subjects(subjectid)
    )
    """

    create_lunches_query = """
    CREATE TABLE lunches (
        lunchid integer generated always as identity,
        name text not null,
        date timestamp not null,

        primary key(lunchid)
    )
    """

    create_student_lunch_choices = """
    CREATE TABLE student_lunch_choice (
        choiceid integer generated always as identity,
        studentid integer,
        lunchid integer,

        primary key(choiceid),

        constraint studentid
        foreign key(studentid) references students(studentid),

        constraint lunchid
        foreign key(lunchid) references lunches(lunchid)

    )
    """

    create_teacher_lunch_choices = """
    CREATE TABLE teacher_lunch_choices (
        choiceid integer generated always as identity,
        teacherid integer,
        lunchid integer,

        primary key(choiceid),

        constraint teacherid
        foreign key(teacherid) references teachers(teacherid),

        constraint lunchid
        foreign key(lunchid) references lunches(lunchid)
    )
    """

    create_events_query = """
    CREATE TABLE events (
        eventid integer generated always as identity,
        date timestamp not null,
        name text not null,

        primary key(eventid)
    )
    """

    create_student_subjects = """
    CREATE TABLE student_subjects (
        studentsubjectid integer generated always as identity,
        studentid integer,
        subjectid integer,

        primary key(studentsubjectid),

        constraint studentid
        foreign key(studentid) references students(studentid),

        constraint subjectid
        foreign key(subjectid) references subjects(subjectid)
    )
    """

    create_students_event_query = """
    CREATE TABLE signed_up_student (
        signupid integer generated always as identity,
        eventid integer,
        studentid integer,

        primary key(signupid),

        constraint eventid
        foreign key(eventid) references events(eventid),

        constraint studentid
        foreign key(studentid) references students(studentid)
    )
    """

    cursor.execute(create_students_query)
    cursor.execute(create_parents_query)
    cursor.execute(create_families_query)
    cursor.execute(create_teachers_query)
    cursor.execute(create_subjects_query)
    cursor.execute(create_teacher_subjects_query)
    cursor.execute(create_grades_query)
    cursor.execute(create_missing_student_query)
    cursor.execute(create_substitutions_query)
    cursor.execute(create_lunches_query)
    cursor.execute(create_student_lunch_choices)
    cursor.execute(create_teacher_lunch_choices)
    cursor.execute(create_events_query)
    cursor.execute(create_student_subjects)
    cursor.execute(create_students_event_query)

def str_time_prop(start, end, format, prop):
    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, '%d/%m/%Y', prop)

def get_random_trip_name():
    random_country = random.choice(pycountry.countries.search_fuzzy("")).name
    return "Trip to %s" % (random_country)

def generate_data(cursor, amount = 10):
    if amount < 5:
        print("amount should be higher than 5!")
    
    food_names = ["Anchovy and italian sausage salad", "Cabbage and horseradish wontons",
            "Falafel and onion panini", "Feijoa and adobo seasoning salad", 
            "Spinach and crab madras", "Tomato and iskender salad",
            "Basil and broccoli stir fry", "Pecan and apple cupcakes",
            "Semolina and pineapple cake", "Marjoram and potato stir fry",
            "Currant and blackcurrant pudding", "Brie and cucumber ciabatta",
            "Pigeon and veal parcels", "Spring onion and manchego salad",
            "Cavatelli and kaffir lime leaf salad", "Steak sandwich with onion relish"]

    subjects = ["Biology", "Geography", "English", "French", "German", "Spanish", 
                "Mathematics", "Chemistry", "Art", "History", "Geometry", "Computer Science"]

    # generate students, teachers, parents, lunches, events
    for i in range(amount):
        cursor.execute("""
        INSERT INTO students(name)
        VALUES (%s)
        """, (names.get_full_name(),))

        cursor.execute("""
        INSERT INTO parents(name)
        VALUES (%s)
        """, (names.get_full_name(),))

        cursor.execute("""
        INSERT INTO teachers(name, pay)
        VALUES (%s, %s)
        """, (names.get_full_name(), str(random.randint(0, 10_000))))

        cursor.execute("""
        INSERT INTO lunches(name, date)
        VALUES (%s, %s)
        """, (random.choice(food_names), random_date("1/1/2020", "1/12/2020", random.random())))

        cursor.execute("""
        INSERT INTO events(name, date)
        VALUES (%s, %s)
        """, (get_random_trip_name(), random_date("1/1/2021", "1/1/2022", random.random())))
    print("generated students, teachers, parents, lunches and events")
    
    # generate families, subjects, sign up students on events
    # make student and teacher lunch choices
    for i in range(amount):
        cursor.execute("""
        SELECT studentid FROM students
        ORDER BY RANDOM() LIMIT 1
        """)

        random_student = cursor.fetchone()[0]

        cursor.execute("""
        SELECT teacherid FROM teachers
        ORDER BY RANDOM() LIMIT 1
        """)

        random_teacher = cursor.fetchone()[0]

        cursor.execute("""
        SELECT parentid FROM parents
        ORDER BY RANDOM() LIMIT 1
        """)

        random_parent = cursor.fetchone()[0]

        cursor.execute("""
        SELECT eventid FROM events
        ORDER BY RANDOM() LIMIT 1
        """)

        random_event = cursor.fetchone()[0]

        cursor.execute("""
        SELECT lunchid FROM lunches
        ORDER BY RANDOM() LIMIT 1
        """)

        random_lunch = cursor.fetchone()[0]

        cursor.execute("""
        INSERT INTO families(parentid, studentid)
        VALUES (%s, %s)
        """, (random_parent, random_student))

        cursor.execute("""
        INSERT INTO signed_up_student(studentid, eventid)
        VALUES (%s, %s)
        """, (random_student, random_event))

        cursor.execute("""
        INSERT INTO student_lunch_choice(studentid, lunchid)
        VALUES (%s, %s)
        """, (random_student, random_lunch))

        cursor.execute("""
        INSERT INTO teacher_lunch_choices(teacherid, lunchid)
        VALUES (%s, %s)
        """, (random_teacher, random_lunch))

        cursor.execute("""
        INSERT INTO subjects(teacherid, name)
        VALUES (%s, %s)
        """, (random_teacher, random.choice(subjects)))
    print("generated families, subjects, signed up students on events, made student and teacher lunch choices")

    # generate substitutions
    cursor.execute("""
    select teacherid, name from teachers
    """)
    all_teachers = cursor.fetchall()
    for teacher in all_teachers:
        teacher_name = teacher[1]
        teacher_id = teacher[0]

        for i in range(amount):
            cursor.execute("""
            SELECT subjectid FROM subjects
            ORDER BY RANDOM() LIMIT 1
            """)
            random_subject = cursor.fetchone()[0]

            substituting_teacher = random.choice(all_teachers)
            if (substituting_teacher[0] == teacher_id):
                while substituting_teacher[0] == teacher_id:
                    substituting_teacher = random.choice(all_teachers)
            
            cursor.execute("""
            INSERT INTO substitutions(teacherid, subjectid, 
                                    substituteteacherid, date)
            VALUES (%s, %s, %s, %s)
            """, (str(teacher_id), str(random_subject),
                str(substituting_teacher[0]), random_date("1/1/2020", "1/12/2020", random.random())))
    print("generated substitutions")

    # generate missing students
    cursor.execute("""
    select studentid, name from students
    """)

    all_students = cursor.fetchall()
    for student in all_students:
        student_name = student[1]
        student_id = student[0]

        for i in range(amount):
            cursor.execute("""
            SELECT subjectid FROM subjects
            ORDER BY RANDOM() LIMIT 1
            """)
            random_subject = cursor.fetchone()[0]

            cursor.execute("""
            INSERT INTO missingstudents(subjectid, studentid, date)
            VALUES (%s, %s, %s)
            """, (str(random_subject), str(student_id), 
                      random_date("1/1/2020", "1/12/2020", random.random())))
    print("generated missing student entries")

    # generate grades
    cursor.execute("""
    select subjectid from subjects
    """)

    all_subjects = cursor.fetchall()
    for student in all_students:
        for subject in all_subjects:
            for i in range(amount):
                student_id = student[0]
                subject_id = subject[0]

                cursor.execute("""
                INSERT INTO grades(subjectid, studentid, grade)
                VALUES (%s, %s, %s)
                """, (str(subject_id), str(student_id), random.randint(1, 5)))
    print("generated grades")


    # generate teacher subjects
    for teacher in all_teachers:
        for i in range(amount):
            teacher_id = teacher[0]

            cursor.execute("""
            SELECT subjectid FROM subjects
            ORDER BY RANDOM() LIMIT 1
            """)
            random_subject = cursor.fetchone()[0]

            cursor.execute("""
            INSERT INTO teacher_subjects(teacherid, subjectid)
            VALUES (%s, %s)
            """, (str(teacher_id), str(random_subject)))
    print("assigned subjects to teachers")

# get the teacher that has a subject
# with the lowest grade average
def get_worst_teacher(cursor):
    cursor.execute("""
    SELECT sbid, grade_average, ts.subjectid, t.name, ts.teacherid, sbs.name
    FROM (select avg(grade) as grade_average, subjectid as sbid 
          from grades GROUP BY studentid, subjectid) as sb, teachers t, teacher_subjects ts, subjects sbs
    WHERE sbid=ts.subjectid and t.teacherid=sbid and sbs.subjectid=sbid 
    GROUP BY ts.teacherid, sbid, sb.grade_average, ts.subjectid, t.name, sbs.name 
    ORDER BY grade_average DESC LIMIT 1
    """)
    return cursor.fetchone()

# get the teacher that has a subject
# with the most missing students
def get_most_boring_teacher(cursor):
    cursor.execute("""
    SELECT missing_student_count, t.name, sbs.name
    FROM (SELECT count(ms.missingstudentid) as missing_student_count, ms.subjectid as subject_id 
          FROM missingstudents ms GROUP BY subject_id) as mstd, teachers t, teacher_subjects ts, subjects sbs
    WHERE ts.subjectid=sbs.subjectid and subject_id=ts.subjectid
    ORDER BY missing_student_count DESC
    LIMIT 1
    """)
    return cursor.fetchone()

# get the family with the highest amount
# of members (parent with most children)
def get_largest_family(cursor):
    cursor.execute("""
    SELECT p.name as parent_name, s.name as student_name, count(f.studentid) as family_count
    FROM families f, parents p, students s
    WHERE f.parentid=p.parentid and f.studentid=s.studentid 
    GROUP BY f.studentid, p.name, s.name 
    ORDER BY family_count DESC
    LIMIT 1
    """)
    return cursor.fetchone()

# get the event which has the most signed up students
def get_event_with_most_students(cursor):
    cursor.execute("""
    SELECT count(e.studentid) as student_count, ev.name
    FROM signed_up_student e, events ev
    WHERE ev.eventid=e.eventid
    GROUP BY ev.name 
    ORDER BY student_count DESC
    LIMIT 1
    """)
    return cursor.fetchone()

# get the lunch entry that has most
# people (teachers and students) signed up for
def get_best_food(cursor):
    cursor.execute("""
    SELECT (count(sl.studentid) + count(tl.teacherid)) as count_all, count(sl.studentid) as count_students, count(tl.teacherid) as count_teachers, l.name
    FROM student_lunch_choice sl, teacher_lunch_choices tl, lunches l
    WHERE sl.lunchid=l.lunchid and tl.lunchid=l.lunchid
    GROUP BY l.lunchid
    ORDER BY count_all DESC
    LIMIT 1
    """)
    return cursor.fetchone()

database, cursor = connect_to_db("dbuser", "password")

create_tables(cursor)
database.commit()

print("created tables!")

generate_data(cursor, 15)
database.commit()
print("generated data!")

print("\n===========================================\n")

worst_teacher = get_worst_teacher(cursor)
print("The worst teacher is: %s with grade average %f on subject %s." % (worst_teacher[3], worst_teacher[1], worst_teacher[5]))
most_boring_teacher = get_most_boring_teacher(cursor)
print("The most boring teacher is: %s with %d missing students with subject %s." % (most_boring_teacher[1], 
                                                                                   most_boring_teacher[0], 
                                                                                   most_boring_teacher[2]))
largest_family = get_largest_family(cursor)
print("The parent with most children is %s with %d children." % (largest_family[0], largest_family[2]))
event_with_most_students = get_event_with_most_students(cursor)
print("The event with most students signed up is %s with %d students signed up." % (event_with_most_students[1], 
                                                                                    event_with_most_students[0]))
lunch_with_most_orders = get_best_food(cursor)
print("The lunch that most people are signed up for is %s with %d people signed up %d students and %d teachers" % (lunch_with_most_orders[3],
                                                                                                                     lunch_with_most_orders[0],
                                                                                                                     lunch_with_most_orders[1],
                                                                                                                     lunch_with_most_orders[2]))


database.close()