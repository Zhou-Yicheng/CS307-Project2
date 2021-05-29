from exception import EntityNotFoundError, IntegrityViolationError
import asyncpg
import datetime
from service.student_service import StudentService
from typing import List, Mapping, Optional

from dto import (Course, CourseGrading, CourseSearchEntry, CourseSection,
                 CourseTable, CourseType,
                 DayOfWeek, EnrollResult, Grade, Major, PassOrFailGrade)


class student_service(StudentService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    # * Course full name: String.format("%s[%s]", course.name, section.name)
    # *
    # * Course conflict is when multiple sections belong to the same course.
    # * Time conflict is when multiple sections have time-overlapping classes.
    # * Note that a section is both course and time conflicting with itself!

    async def add_student(self, user_id: int, major_id: int, first_name: str,
                          last_name: str, enrolled_date: datetime.date):
        async with self.__pool.acquire() as con:
            if str.isalpha(first_name) and str.isalpha(last_name):
                full_name = first_name + ' ' + last_name
            else:
                full_name = last_name+first_name
            try:
                await con.excute('''
                insert into student (id, full_name, enrolled_date, major)
                values (%d, %s, %s, %d)
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
                            ignore_full: bool, ignore_conflict: bool,
                            ignore_passed: bool,
                            ignore_missing_prerequisites: bool,
                            page_size: int, page_index: int
                            ) -> List[CourseSearchEntry]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from course
                join section on course.id = section.course
                join semester on section.semester = semester.id
                    having semester = %d
                join takes on section.id = section_id
                    having student_id = %d
            ''' % (semester_id, student_id))
            if res:
                return [CourseSearchEntry(
                                          Course(r['course.id'],
                                                 r['course.name'],
                                                 r['credit'],
                                                 r['class_hout'],
                                                 CourseGrading[r['grading']]
                                                 ),
                                          CourseSection(r['section'],
                                                        r['section.name'],
                                                        r['total_capacity'],
                                                        r['left_capacity']
                                                        ),
                                          [],
                                          []
                        ) for r in res]
            else:
                raise EntityNotFoundError

    async def enroll_course(self, student_id: int, section_id: int) \
            -> EnrollResult:
        async with self.__pool.acquire() as con:
            try:
                res = await con.execute('''
                insert into takes (student_id, section_id)
                values (%d, %d)
                ''' % (student_id, section_id))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    # TODO: capacity
    async def drop_course(self, student_id: int, section_id: int):
        async with self.__pool.acquire() as con:
            grade = await con.fetchval('''
            select grade
            from takes
            where studnet_id = %d and section_id = %d
            ''' % (student_id, section_id))
            if grade:
                raise RuntimeError
            else:
                res = await con.execute('''
                delete from takes
                where student_id = %d and section_id = %d
                ''' % (student_id, section_id))
                if res == 'DELETE 0':
                    raise EntityNotFoundError

    async def add_enrolled_course_with_grade(self, student_id: int,
                                             section_id: int,
                                             grade: Optional[Grade]):
        if grade:
            async with self.__pool.acquire() as con:
                grading = await con.fetchval('''
                select grading
                from course
                    join section on course.id = section.course
                where section = %d
                ''' % section_id)
                # if (grading == 'PASS_OR_FAIL' and isinstance(grade, int)
                #     or grading == 'HUNDRED_MARK_SCORE' and
                #                   isinstance(grade, PassOrFailGrade)):
                #     raise BaseException
                if grading == 'PASS_OR_FAIL':
                    if grade == PassOrFailGrade.PASS:
                        
                    elif grade == PassOrFailGrade.FAIL:
                elif grading == 'HUNDRED_MARK_SCORE':
                    pass

    async def set_enrolled_course_grade(self, student_id: int,
                                        section_id: int, grade: Grade):
        async with self.__pool.acquire() as con:
            if isinstance(grade, PassOrFailGrade.PASS):
                grade = 'PASS'
            elif isinstance(grade, PassOrFailGrade.FAIL):
                grade = 'FAIL'
            else:
                await con.execute('''
                update takes
                set grade = '%s'
                where student_id = %d and section_id = %d
                ''' % (grade, student_id, section_id))

    async def get_enrolled_courses_and_grades(self, student_id: int,
                                              semester_id: Optional[int]) \
            -> Mapping[Course, Grade]:
        raise NotImplementedError

    async def get_course_table(self, student_id: int, date: datetime.date) \
            -> CourseTable:
        raise NotImplementedError

    async def passed_prerequisites_for_course(self, student_id: int,
                                              course_id: str) -> bool:
        raise NotImplementedError

    async def get_student_major(self, student_id: int) -> Major:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('''
            select *
            from major
            where id = (select major from student where id = %d)
            ''' % student_id)
            if (res):
                return Major(res['id'], res['name'], res['department'])
            else:
                raise EntityNotFoundError
