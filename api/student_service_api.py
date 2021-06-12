import datetime
from typing import List, Mapping, Optional

import asyncpg
from dto import (Course, CourseGrading, CourseSearchEntry, CourseSection,
                 CourseSectionClass, CourseTable, CourseTableEntry, CourseType,
                 DayOfWeek, Department, EnrollResult, Grade, Instructor, Major,
                 PassOrFailGrade, )
from exception import EntityNotFoundError, IntegrityViolationError
from service.student_service import StudentService


class student_service(StudentService):
    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool
        self.__cache = {}

    async def add_student(self,
                          user_id: int,
                          major_id: int,
                          first_name: str,
                          last_name: str,
                          enrolled_date: datetime.date):
        async with self.__pool.acquire() as con:
            try:
                if str.isascii(first_name) and str.isascii(last_name):
                    full_name = first_name+' '+last_name
                else:
                    full_name = first_name+last_name
                await con.execute('''
                insert into student (id, full_name, enrolled_date, major)
                values (%d, '%s', '%s', %d)
                ''' % (user_id, full_name, enrolled_date, major_id))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def search_course(self, *,
                            student_id: int,
                            semester_id: int,
                            search_cid: Optional[str] = None,
                            search_name: Optional[str] = None,
                            search_instructor: Optional[str] = None,
                            search_day_of_week: Optional[DayOfWeek] = None,
                            search_class_time: Optional[int] = None,
                            search_class_locations: List[str] = None,
                            search_course_type: CourseType,
                            ignore_full: bool,
                            ignore_conflict: bool,
                            ignore_passed: bool,
                            ignore_missing_prerequisites: bool,
                            page_size: int,
                            page_index: int
                            ) -> List[CourseSearchEntry]:
        async with self.__pool.acquire() as con:
            stu = await con.fetchrow('select * from student where id = %d'
                                     % student_id)
            if stu is None:
                raise EntityNotFoundError(student_id)
            HEAD = 'select * from schedule'
            BODY = '''
            where semester = %d ''' % semester_id
            if search_cid:
                BODY += "and cid like '%%%s%%' " % search_cid
            if search_name:
                BODY += "and _name like '%%%s%%' " % search_name
            if search_instructor:
                BODY += '''
                and exists(
                    select null
                    from (select ins, generate_subscripts(ins, 1) as i) i
                    where ins[i].full_name like '%%%s%%'
                ) ''' % search_instructor
            if search_day_of_week:
                BODY += '''
                and exists(
                    select null
                    from (select cls, generate_subscripts(cls, 1) as i) i
                    where cls[i].day_of_week = '%s'
                ) ''' % search_day_of_week.name
            if search_class_time:
                BODY += '''
                and exists(
                    select null
                    from (select cls, generate_subscripts(cls, 1) as i) i
                    where %d between cls[i].class_begin and cls[i].class_end
                ) ''' % search_class_time
            if search_class_locations:
                BODY += '''
                and exists(
                    select null
                    from (select cls, generate_subscripts(cls, 1) as i) i
                    where cls[i].location ~ ANY(array%s)
                ) ''' % search_class_locations
            if search_course_type is CourseType.ALL:
                pass
            if search_course_type is CourseType.MAJOR_COMPULSORY:
                BODY += '''
                and cid in (
                    select course_id
                    from major_course
                    where major_id = %d and course_type = 'C'
                ) ''' % stu['major']
            if search_course_type is CourseType.MAJOR_ELECTIVE:
                BODY += '''
                and cid in (
                    select course_id
                    from major_course
                    where major_id = %d and course_type = 'E'
                ) ''' % stu['major']
            if search_course_type is CourseType.CROSS_MAJOR:
                BODY += '''
                and cid in (
                    select course_id
                    from major_course
                    where major_id <> %d
                ) ''' % stu['major']
            if search_course_type is CourseType.PUBLIC:
                BODY += 'and cid NOT in \
                    (select course_id from major_course) '
            if ignore_full:
                BODY += 'and left_capacity > 0 '
            if ignore_passed:
                BODY += '''
                and sid NOT in (
                    select section_id
                    from takes
                    where student_id = %d
                        and grade <> 'FAIL'
                        and (grade = 'PASS' or cast(grade as integer) >= 60)
                ) ''' % student_id
            if ignore_missing_prerequisites:
                BODY += 'and pass_pre(%d, cid) ' % student_id
            if ignore_conflict:
                BODY += '''
                and NOT exists (
                    select null
                    from takes
                        join section on section_id = section.id
                        join class c on section.id = c.section
                    where student_id = %d
                      and semester = %d
                      and (course = cid
                       or exists(
                          select null
                          from (select cls, generate_subscripts(cls, 1) as i) i
                          where cls[i].week_list && c.week_list
                            and cls[i].day_of_week = c.day_of_week
                            and cls[i].class_begin <= c.class_end
                            and cls[i].class_end >= c.class_begin
                      ))
                ) ''' % (student_id, semester_id)

            TAIL = '''
            order by cid, _name
            limit %d offset %d
            ''' % (page_size, page_size*page_index)

            try:
                stm = await con.prepare(HEAD+BODY+TAIL)
            except asyncpg.exceptions.SyntaxOrAccessError as e:
                print(HEAD+BODY+TAIL)
                print(e)

            res = await stm.fetch()

            if not res:
                return []
            else:
                ans = []
                for r in res:
                    ins = {i['id']: Instructor(i['id'], i['full_name']) for i in r['ins']}
                    cos = Course(r['cid'],
                                 r['cname'],
                                 r['credit'],
                                 r['hour'],
                                 CourseGrading[r['grading']])
                    sec = CourseSection(r['sid'],
                                        r['sname'],
                                        r['total_capacity'],
                                        r['left_capacity'])
                    cls = [CourseSectionClass(c['id'],
                                              ins[c['instructor']],
                                              DayOfWeek[c['day_of_week']],
                                              c['week_list'],
                                              c['class_begin'],
                                              c['class_end'],
                                              c['location']
                                              ) for c in r['cls']]
                    if ignore_conflict:
                        conf = []
                    else:
                        conf = await con.fetch('''
                        select distinct course.name||'['||section.name||']' as _name
                        from takes
                            join section on section_id = section.id
                            join class on section.id = class.section
                            join course on section.course = course.id
                        where student_id = %d and semester = %d
                          and (
                              course = '%s'
                              or exists (
                                  select null
                                  from class this
                                  where section = %d
                                    and week_list && this.week_list
                                    and day_of_week = this.day_of_week
                                    and class_begin <= this.class_end
                                    and class_end >= this.class_begin
                              )
                          )
                        order by _name
                        ''' % (student_id, semester_id, r['cid'], r['sid']))
                        conf = [c['_name'] for c in conf]

                    ans.append(CourseSearchEntry(cos, sec, cls, conf))
                return ans

    async def enroll_course(self,
                            student_id: int,
                            section_id: int) -> EnrollResult:
        async with self.__pool.acquire() as con:
            try:
                sec = await con.fetchrow('''
                select * from section where id = %d
                ''' % section_id)
                if sec is None:
                    return EnrollResult.COURSE_NOT_FOUND

                took = await con.fetchrow('''
                select null
                from takes
                where student_id = %d and section_id = %d
                ''' % (student_id, section_id))
                if took:
                    return EnrollResult.ALREADY_ENROLLED

                grade = await con.fetch('''
                select grade
                from takes
                where student_id = %d
                  and section_id in (select id from section where course ='%s')
                ''' % (student_id, sec['course']))
                if (grade and grade[-1][0]):
                    if grade[-1][0] == 'PASS':
                        return EnrollResult.ALREADY_PASSED
                    if grade[-1][0] != 'FAIL' and int(grade[-1][0]) >= 60:
                        return EnrollResult.ALREADY_PASSED

                pas = await con.fetchval("select pass_pre(%d, '%s')"
                                         % (student_id, sec['course']))
                if not pas:
                    return EnrollResult.PREREQUISITES_NOT_FULFILLED

                conf = await con.fetch('''
                select null
                from takes
                    join section on section_id = section.id
                    join class on section.id = class.section
                where student_id = %d and semester = %d
                  and (
                       course = '%s'
                       or exists (
                          select null
                          from class this
                              join section on this.section = %d
                          where this.week_list && class.week_list
                            and this.day_of_week = class.day_of_week
                            and this.class_begin <= class.class_end
                            and this.class_end >= class.class_begin
                       )
                  )
                ''' % (student_id, sec['semester'], sec['course'], section_id))
                if conf:
                    return EnrollResult.COURSE_CONFLICT_FOUND

                if sec['left_capacity'] == 0:
                    return EnrollResult.COURSE_IS_FULL

                async with con.transaction() as tr:
                    await con.execute('''
                    insert into takes (student_id, section_id)
                    values (%d, %d)
                    ''' % (student_id, section_id))
                    await con.execute('''
                    update section
                    set left_capacity = left_capacity - 1
                    where id = %d
                    ''' % (section_id))

                    capacity = await con.fetchval('''
                        select left_capacity
                        from section
                        where id = %d
                    ''' % (section_id))
                    if capacity >= 0:
                        return EnrollResult.SUCCESS
                    else:
                        await tr.rollback()
                        return EnrollResult.COURSE_IS_FULL
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def drop_course(self,
                          student_id: int,
                          section_id: int):
        async with self.__pool.acquire() as con:
            if (student_id, section_id) in self.__cache:
                grade = self.__cache[(student_id, section_id)]
            else:
                res = await con.fetchrow('''
                select grade from takes
                where student_id = %d and section_id = %d
                ''' % (student_id, section_id))
                if res is None:
                    raise EntityNotFoundError
                grade = res['grade']
            if grade is not None:
                raise RuntimeError
            async with con.transaction():
                await con.execute('''
                delete from takes
                where student_id = %d and section_id = %d
                ''' % (student_id, section_id))
                await con.execute('''
                update section
                set left_capacity = left_capacity+1
                where id = %d
                ''' % section_id)

    async def add_enrolled_course_with_grade(self,
                                             student_id: int,
                                             section_id: int,
                                             grade: Optional[Grade]):
        async with self.__pool.acquire() as con:
            try:
                if grade is None:
                    await con.execute('''
                    insert into takes (student_id, section_id)
                    values (%d, %d)
                    ''' % (student_id, section_id))
                else:
                    grading = await con.fetchval('''
                    select grading
                    from course
                    where id = (select course from section where section.id=%d)
                    ''' % section_id)
                    if (grading is None):
                        raise EntityNotFoundError
                    if (grading == 'PASS_OR_FAIL' and type(grade) == int or
                            grading == 'HUNDRED_MARK_SCORE' and
                            isinstance(grade, PassOrFailGrade)):
                        raise IntegrityViolationError

                    if type(grade) == int:
                        grade = str(grade)
                    else:
                        grade = grade.name
                    await con.execute('''
                    insert into takes (student_id, section_id, grade)
                    values (%d, %d, '%s')
                    ''' % (student_id, section_id, grade))
                    self.__cache[(student_id, section_id)] = grade
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def set_enrolled_course_grade(self,
                                        student_id: int,
                                        section_id: int,
                                        grade: Grade):
        async with self.__pool.acquire() as con:
            if type(grade) == int:
                grade = str(grade)
            else:
                grade = grade.name
            res = await con.execute('''
            update takes
            set grade = '%s'
            where student_id = %d and section_id = %d
            ''' % (grade, student_id, section_id))
            if res == 'UPDATE 0':
                raise EntityNotFoundError
            else:
                self.__cache[(student_id, section_id)] = grade

    async def get_enrolled_courses_and_grades(self,
                                              student_id: int,
                                              semester_id: Optional[int]) \
            -> Mapping[Course, Grade]:
        async with self.__pool.acquire() as con:
            if semester_id:
                res = await con.fetch('''
                select course.*, grade
                from course
                    join section on course.id = section.course and semester =%d
                    join takes on section.id = section_id and student_id = %d
                ''' % (semester_id, student_id))
            else:
                res = await con.fetch('''
                select course.*, grade
                from course
                    join section on course.id = section.course
                    join takes on section.id = section_id and student_id = %d
                    join semester on section.semester = semester.id
                order by begin_date
                ''' % student_id)
            if res:
                return {Course(r['id'],
                               r['name'],
                               r['credit'],
                               r['class_hour'],
                               CourseGrading[r['grading']]
                               ): student_service.dto(r['grade']) for r in res}
            else:
                raise EntityNotFoundError

    def dto(grade: str) -> Grade:
        if grade is None:
            return None
        elif grade == 'PASS':
            grade = PassOrFailGrade.PASS
        elif grade == 'FAIL':
            grade = PassOrFailGrade.FAIL
        else:
            grade = int(grade)
        return grade

    async def get_course_table(self,
                               student_id: int,
                               date: datetime.date) -> CourseTable:
        async with self.__pool.acquire() as con:
            semester, week = await con.fetchval('''
            select day_in_semester_week('%s')
            ''' % date)
            if semester is None or week is None:
                return {day: [] for day in DayOfWeek}

            res = await con.fetch('''
            select day_of_week,
                   course.name || '[' || section.name || ']' as class_name,
                   instructor,
                   full_name,
                   class_begin,
                   class_end,
                   location
            from class
                join section on section = section.id and semester = %d
                join takes on section = section_id and student_id = %d
                join course on section.course = course.id
                join instructor on instructor = instructor.id
            where %d = ANY(week_list)
            ''' % (semester, student_id, week))
            table = {day: [] for day in DayOfWeek}
            for r in res:
                day = DayOfWeek[r['day_of_week']]
                entry = CourseTableEntry(r['class_name'],
                                         Instructor(r['instructor'],
                                                    r['full_name']),
                                         r['class_begin'],
                                         r['class_end'],
                                         r['location'])
                table[day].append(entry)
            return table

    async def passed_prerequisites_for_course(self,
                                              student_id: int,
                                              course_id: str) -> bool:
        return await self.__pool.acquire().fetchval("select pass_pre(%d, '%s'"
                                                    % student_id, course_id)
        # async with self.__pool.acquire() as con:
        #     res = await con.fetch('''
        #     select *
        #     from prerequisite
        #     where id = '%s'
        #     order by idx
        #     ''' % course_id)
        #     if not res:
        #         return True
        #     pas = await con.fetch('''
        #     select course
        #     from takes
        #         join section on section_id = section.id
        #     where student_id = %d
        #       and grade <> 'FAIL'
        #       and (grade = 'PASS' or cast(grade as integer) >= 60)
        #     ''' % student_id)
        #     pas = [str(t['course']) for t in pas]

        #     stack = [0]
        #     val = [False for i in range(len(res))]
        #     visited = [False for i in range(len(res))]
        #     while stack:
        #         i = stack[-1]
        #         if visited[i]:
        #             if res[i]['val'] == 'AND':
        #                 val[i] = all([val[c] for c in res[i]['ptr']])
        #                 stack.pop()
        #             elif res[i]['val'] == 'OR':
        #                 val[i] = any([val[c] for c in res[i]['ptr']])
        #                 stack.pop()
        #         else:
        #             visited[i] = True
        #             if res[i]['ptr']:
        #                 for j in res[i]['ptr']:
        #                     stack.append(j)
        #             else:
        #                 val[i] = (res[i]['val'] in pas)
        #                 stack.pop()
        #     return val[0]

    async def get_student_major(self,
                                student_id: int) -> Major:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('''
            select major.id, major.name as major_name, department,
                department.name as department_name
            from major
                join department on department = department.id
            where major.id = (select major from student where student.id = %d)
            ''' % student_id)
            if res:
                return Major(res['id'],
                             res['major_name'],
                             Department(res['department'],
                                        res['department_name']
                                        )
                             )
            else:
                raise EntityNotFoundError
