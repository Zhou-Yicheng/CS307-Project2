drop schema public CASCADE;
create schema public;
create table semester (
	id			serial primary key,
	name			varchar not null,
	begin_date		date not null,
	end_date		date not null,
	CHECK (begin_date < end_date),
	UNIQUE (name, begin_date)
);

create table department (
	id			serial primary key,
	name			varchar not null,
	UNIQUE(name)
);

create table major (
	id			serial primary key,
	name			varchar not null,
	department		integer not null references department ON DELETE CASCADE,
	UNIQUE(name, department)
);

create table instructor (
	id			integer primary key,
	full_name		varchar not null
);

create table student (
	id			integer primary key,
	full_name		varchar not null,
	enrolled_date		date not null,
	major			integer not null references major ON DELETE CASCADE
);

create table course (
	id			varchar primary key,
	name			varchar not null,
	credit			integer not null,
	class_hour		integer not null,
	grading 		varchar not null,
	CHECK (grading in ('PASS_OR_FAIL', 'HUNDRED_MARK_SCORE'))
);

-- section means class, name could be "No.1 Chinese class", "No.1 English class"
create table section (
	id			serial primary key,
	course			varchar not null references course ON DELETE CASCADE,
	semester		integer not null references semester ON DELETE CASCADE,
	name			varchar not null,
	total_capacity		integer not null,
	left_capacity		integer not null,
	UNIQUE (course, semester, name)
);

-- class means lecture
create table class (
	id			serial primary key,
	section			integer not null references section ON DELETE CASCADE,
	instructor		integer not null references instructor ON DELETE CASCADE,
	day_of_week		varchar not null,
	week_list		integer[] not null,
	class_begin		integer not null,
	class_end		integer not null,
	location		varchar not null,
	CHECK (class_begin < class_end),
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
	course_type		char(1) not null,
	CHECK (course_type in ('C', 'E')),
	PRIMARY KEY (major_id, course_id)
);

-- TODO: index
create index on class (section);
create index on section (course);
create index on prerequisite (id);
create index on takes (student_id);

-- create view coursetable as(
-- 	select day_of_week,
-- 		   course.name||'['||section.name||']' as class_name,
-- 		   instructor.id,
-- 		   instructor.name,
-- 		   class_begin,
-- 		   class_end,
-- 		   location
-- 	from class
-- 		join section on section = section.id
-- 		join course on course = course.id
-- );

create or replace function day_in_semester_week(IN day date, OUT semester_id integer, OUT week integer)
as $$
begin

select id into semester_id
from semester
where day between begin_date and end_date;

if(semester_id is null)
then return;
end if;

select (day - begin_date)/7 + 1 into week
from semester
where id = semester_id;

return;
end
$$ language plpgsql

create or replace function pass_pre(sid integer, course_id varchar)
    returns boolean
AS $$
    declare
        res "prerequisite"[];
        pas varchar[];
        val boolean[];
        visited boolean[];
        stack integer[];
        top integer;
        i integer;
        ptri integer;
        pasi varchar;
        all_flag boolean;
        any_flag boolean;
begin
    select array_agg(x) into res
    from
        (select *
        from prerequisite
        where id = course_id
        order by idx) x;
    if array_length(res,1) is null then
        return true;
    end if;

    select array_agg(x) into pas
    from
        (select course
        from takes
            join section on section_id = section.id
        where student_id = sid
            and grade <> 'FAIL'
            and (grade = 'PASS' or cast(grade as integer) >= 60)) x;

    top := 1;
    stack[top] := 0;
    for x in 1..array_length(res,1) loop
        val[x] := false;
        visited[x] := false;
        end loop;
    while top >= 1 loop
        i := stack[top];
        if visited[i] then
            if res[i].val = 'AND' then
                all_flag := true;
                foreach ptri in array res[i].ptr loop
                    all_flag := val[ptri] and all_flag;
                    end loop;
                val[i] := all_flag;
                top := top-1;
            elseif res[i].val = 'OR' then
                any_flag := false;
                foreach ptri in array res[i].ptr loop
                    any_flag := val[ptri] or any_flag;
                    val[i] := any_flag;
                    end loop;
            end if;
        else
            visited[i] := true;
            if res[i].ptr is not null then
                foreach ptri in array res[i].ptr loop
                    top := top + 1;
                    stack[top] := ptri;
                    end loop;
            else
                val[i] := false;
                foreach pasi in array pas loop
                    if(res[i].val = pasi) then
                        val[i] := true;
                    end if;
                    end loop;
                top := top - 1;
            end if;
        end if;
        end loop;
    return val[1];
end;
$$ language plpgsql;