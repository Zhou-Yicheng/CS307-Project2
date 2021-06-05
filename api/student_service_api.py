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

    async def add_student(self, user_id: int, major_id: int, first_name: str,
                          last_name: str, enrolled_date: datetime.date):
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

    async def search_course(self, *, student_id: int, semester_id: int,
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
                            page_size: int, page_index: int
                            ) -> List[CourseSearchEntry]:
        async with self.__pool.acquire() as con:
            return []
            sql = '''
            select course.id as course_id, course.name as course_name, credit,
                class_hour, grading, section.id as section_id, section.name as
                section_name, total_capacity, left_capacity, array_agg(class)
            from course
                join section on course.id = section.course and semester = %d
                join class on section.id = class.section
            ''' % semester_id
            if (search_cid or search_name or search_instructor or
                search_day_of_week or search_class_time or
                search_class_locations or search_course_type or
                ignore_full or ignore_conflict or ignore_passed or
                    ignore_missing_prerequisites):
                sql += 'where 1=1 '
            sql += '''
            group by (course_id, course_name, credit, class_hour, grading,
                section_id, section_name, total_capacity, left_capacity)
            order by course_id, course_name, section_name
            '''
            res = await con.fetch(sql)
            if res:
                ans = []
                for r in res:
                    cos = Course(r['course_id'],
                                 r['course_name'],
                                 r['credit'],
                                 r['class_hour'],
                                 CourseGrading[r['grading']])
                    sec = CourseSection(r['section_id'],
                                        r['section_name'],
                                        r['total_capacity'],
                                        r['left_capacity'])
                    cls = [CourseSectionClass(c['id'],
                                              Instructor(c['instructor'],
                                                         await con.fetchval('select full_name from instructor where id=%d'%c['instructor'])),
                                              DayOfWeek[c['day_of_week']],
                                              c['week_list'],
                                              c['class_begin'],
                                              c['class_end'],
                                              c['location']
                                              ) for c in r['array_agg']]
                    conflict = []
                    ans.append(CourseSearchEntry(cos, sec, cls, conflict))
                return ans
            else:
                return []

    async def enroll_course(self, student_id: int, section_id: int) \
            -> EnrollResult:
        async with self.__pool.acquire() as con:
            try:
                sec = await con.fetchrow('''
                select * from section where id = %d
                ''' % (section_id))
                if sec is None:
                    return EnrollResult.COURSE_NOT_FOUND

                enroll = await con.fetchrow('''
                select null
                from takes
                where student_id = %d and section_id = %d
                ''' % (student_id, section_id))
                if enroll:
                    return EnrollResult.ALREADY_ENROLLED

                grade = await con.fetch('''
                select grade
                from takes
                where student_id = %d
                  and section_id in (
                                     select id
                                     from section
                                     where course = '%s'
                                     )
                ''' % (student_id, sec['course']))
                if (grade
                    and grade[-1][0]
                    and (grade[-1][0] == PassOrFailGrade.PASS
                         or grade[-1][0] >= 60)):
                    return EnrollResult.ALREADY_PASSED

                pas = await self.passed_prerequisites_for_course(student_id,
                                                                 sec['course'])
                if not pas:
                    return EnrollResult.PREREQUISITES_NOT_FULFILLED

                time = await con.fetch('''
                select day_of_week, class_begin, class_end
                from class
                    join section on section = section.id and semester = %d
                    join takes on section = section_id and student_id = %d
                ''' % (sec['semester'], student_id))
                this = await con.fetch('''
                select day_of_week, class_begin, class_end
                from class
                where section = %d
                ''' % (section_id))
                if time and this:
                    for t in time:
                        for c in this:
                            if t['day_of_week'] == c['day_of_week']:
                                if (t['class_end'] < c['class_begin'] or
                                        t['class_begin'] > c['class_end']):
                                    pass
                                else:
                                    return EnrollResult.COURSE_CONFLICT_FOUND

                take = await con.fetch('''
                select null
                from takes
                    join section on section_id = section.id
                        and semester = %d and course = '%s'
                where student_id = %d
                ''' % (sec['semester'], sec['course'], student_id))
                if take:
                    return EnrollResult.COURSE_CONFLICT_FOUND

                if sec['left_capacity'] == 0:
                    return EnrollResult.COURSE_IS_FULL

                async with con.transaction():
                    await con.execute('''
                    insert into takes (student_id, section_id)
                    values (%d, %d)
                    ''' % (student_id, section_id))
                    await con.execute('''
                    update section
                    set left_capacity = left_capacity - 1
                    where id = %d
                    ''' % (section_id))
                    return EnrollResult.SUCCESS
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def drop_course(self, student_id: int, section_id: int):
        async with self.__pool.acquire() as con:
            take = await con.fetchrow('''
            select *
            from takes
            where student_id = %d and section_id = %d
            ''' % (student_id, section_id))
            if take is None:
                raise EntityNotFoundError
            if take['grade']:
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

    async def add_enrolled_course_with_grade(self, student_id: int,
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
                    if (grading == 'PASS_OR_FAIL' and type(grade) == int or
                            grading == 'HUNDRED_MARK_SCORE' and
                            isinstance(grade, PassOrFailGrade)):
                        raise IntegrityViolationError
                    if grade == PassOrFailGrade.PASS:
                        grade = 'PASS'
                    if grade == PassOrFailGrade.FAIL:
                        grade = 'FAIL'
                    await con.execute('''
                    insert into takes (student_id, section_id, grade)
                    values (%d, %d, '%s')
                    ''' % (student_id, section_id, grade))

            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def set_enrolled_course_grade(self, student_id: int,
                                        section_id: int, grade: Grade):
        async with self.__pool.acquire() as con:
            if grade == PassOrFailGrade.PASS:
                grade = 'PASS'
            if grade == PassOrFailGrade.FAIL:
                grade = 'FAIL'
            res = await con.execute('''
            update takes
            set grade = '%s'
            where student_id = %d and section_id = %d
            ''' % (grade, student_id, section_id))
            if res == 'UPDATE 0':
                raise EntityNotFoundError

    async def get_enrolled_courses_and_grades(self, student_id: int,
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
                               ): self.dto(r['grade']) for r in res}
            else:
                raise EntityNotFoundError

    def dto(grade: str) -> Grade:
        if grade == 'PASS':
            grade = PassOrFailGrade.PASS
        elif grade == 'FAIL':
            grade = PassOrFailGrade.FAIL
        else:
            grade = int(grade)
        return grade

    async def get_course_table(self, student_id: int, date: datetime.date) \
            -> CourseTable:
        async with self.__pool.acquire() as con:
            semester, week = await con.fetchval('''
            select day_in_semester_week('%s')
            ''' % date)
            if semester is None or week is None:
                raise EntityNotFoundError

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

    async def passed_prerequisites_for_course(self, student_id: int,
                                              course_id: str) -> bool:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from prerequisite
            where id = '%s'
            order by idx
            ''' % course_id)
            if not res:
                return True
            take = await con.fetch('''
            select course
            from takes
                join section on section_id = section.id
            where student_id = %d
            ''' % student_id)
            take = list(take)

            stack = [0]
            val = [0 for i in range(len(res))]
            visited = [False for i in range(len(res))]
            while stack:
                i = stack[-1]
                if visited[i]:
                    if res[i]['val'] == 'AND':
                        val[i] = all([val[c] for c in res[i]['ptr']])
                        stack.pop()
                    elif res[i]['val'] == 'OR':
                        val[i] = any([val[c] for c in res[i]['ptr']])
                        stack.pop()
                else:
                    visited[i] = True
                    if res[i]['ptr']:
                        for j in res[i]['ptr']:
                            stack.append(j)
                    else:
                        val[i] = (res[i]['val'] in take)
                        stack.pop()
            return val[0]

    async def get_student_major(self, student_id: int) -> Major:
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
