
create table department (
	id		serial primary key,
	name	varchar not null,
	UNIQUE(name)
);

create table major (
	id		serial primary key,
	name	varchar not null,
	department integer not null references department,
	UNIQUE(name, department)
);

-- create table user (
-- 	id		int primary key,
-- 	full_name varchar not null
-- )

create table instructor (
	id		integer primary key,
	full_name varchar not null
);

create table student (
	id		integer primary key,
	full_name varchar not null,
	enrolled_date date not null,
	major	integer not null references major
);

-- TODO: course_type, enroll_result
create table course (
	id		varchar primary key,
	name	varchar not null,
	credit	integer not null,
	class_hour	integer not null,
	grading varchar not null,
	-- grading	char(4) not null,
	-- course_type	varchar not null,
	-- CHECK (course_type in ('MAJOR_COMPULSORY', 'MAJOR_ELECTIVE', 'CROSS_MAJOR', 'PUBLIC')),
	CHECK (grading in ('PASS_OR_FAIL', 'HUNDRED_MARK_SCORE'))
);

-- section means class
create table section (
	id		serial primary key,
	name	varchar not null,
	total_capacity	integer not null,
	left_capacity	integer not null,
	UNIQUE (name)
);

create table course_section (
	course_id	varchar references course,
	section_id	integer references section,
	PRIMARY KEY (course_id, section_id)
);

-- class means lecture
create table class (
	id		serial primary key,
	instructor	integer not null references instructor,
	day_of_week	integer not null,
	week_list	integer[] not null,
	class_begin	integer not null,
	class_end	integer not null,
	location	varchar not null,
	CHECK (day_of_week in (1, 2, 3, 4, 5, 6, 7))
	--UNIQUE? 
);

create table section_class (
	section_id	integer references section,
	class_id	integer references class,
	PRIMARY KEY(section_id, class_id)
);

create table semester (
	id		serial primary key,
	name	varchar not null,
	begin_date	date not null,
	end_date		date not null,
	UNIQUE (name, begin_date, end_date)
);

create table semester_course (
	semester_id		integer references semester,
	course_id		varchar references course,
	PRIMARY KEY (semester_id, course_id)
)

--TODO
-- create table prerequisite (
	
-- )

create table takes (
	student_id		integer references student,
	section_id		integer references section,
	PRIMARY KEY (student_id, section_id)
)

create view coursetable as (
	select course.name, course.
)
