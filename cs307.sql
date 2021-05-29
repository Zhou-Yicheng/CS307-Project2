drop schema public CASCADE;
create schema public;
create table semester (
	id			serial primary key,
	name		varchar not null,
	begin_date	date not null,
	end_date	date not null,
	UNIQUE (name, begin_date, end_date)
);

create table department (
	id			serial primary key,
	name		varchar not null,
	UNIQUE(name)
);

create table major (
	id			serial primary key,
	name		varchar not null,
	department	integer not null references department ON DELETE CASCADE,
	UNIQUE(name, department)
);

create table instructor (
	id			integer primary key,
	full_name	varchar not null
);

create table student (
	id			integer primary key,
	full_name	varchar not null,
	enrolled_date date not null,
	major		integer not null references major ON DELETE CASCADE
);

create table course (
	id			varchar primary key,
	name		varchar not null,
	credit		integer not null,
	class_hour	integer not null,
	grading 	varchar not null,
	CHECK (grading in ('PASS_OR_FAIL', 'HUNDRED_MARK_SCORE'))
);

-- section means class, name could be "No.1 Chinese class", "No.1 English class"
create table section (
	id			serial primary key,
	course		varchar not null references course ON DELETE CASCADE,
	semester	integer not null references semester ON DELETE CASCADE,
	name		varchar not null,
	total_capacity	integer not null,
	left_capacity	integer not null,
	UNIQUE (course, semester, name)
);

-- class means lecture
create table class (
	id			serial primary key,
	section		integer not null references section ON DELETE CASCADE,
	instructor	integer not null references instructor ON DELETE CASCADE,
	day_of_week	varchar not null,
	week_list	integer[] not null,
	class_begin	integer not null,
	class_end	integer not null,
	location	varchar not null,
	CHECK (day_of_week in ('MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'))
);

create table prerequisite (
	id			varchar references course ON DELETE CASCADE,
	idx			integer,
	val			varchar,
	ptr			integer[],
	PRIMARY KEY (id, idx)
);

create table takes (
	student_id		integer references student ON DELETE CASCADE,
	section_id		integer references section ON DELETE CASCADE,
	grade			varchar(4),
	CHECK (grade in ('PASS', 'FAIL') or cast(grade as integer) between 0 and 100),
	PRIMARY KEY (student_id, section_id)
);

create table major_course(
	major_id		integer references major ON DELETE CASCADE,
	course_id		varchar references course ON DELETE CASCADE,
	course_type		varchar not null,
	CHECK (course_type in ('C', 'E')),
	PRIMARY KEY (major_id, course_id)
);

-- TODO: index
create index on class (section);
create index on section (course, semester);

-- TODO: major_course

-- create view coursetable as (
-- 	select course.name, course.
-- );

-- Assume same for every week
-- create table coursetable (
-- 	semester_id		integer not null references semester,
-- 	student_id		integer not null,
-- 	day_of_week		integer not null,
-- 	class_begin		integer not null,
-- 	class_end		integer not null,
-- 	location		varchar not null,
-- 	instructor		varchar not null,
-- 	course_name		varchar not null
-- );

create table major_course(
	major_id		integer references major,
	course_id		varchar references course,
	course_type		varchar not null,
	CHECK(course_type in ('C', 'E')),
	PRIMARY KEY(major_id, course_id)
)