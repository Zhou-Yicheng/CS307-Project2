from exception import EntityNotFoundError, IntegrityViolationError
import asyncpg
import datetime
from service.student_service import StudentService
from typing import List, Mapping, Optional

from dto import (Course, CourseGrading, CourseSearchEntry, CourseTable,
                 CourseType,
                 DayOfWeek, EnrollResult, Grade, Major)


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
                return [Course(r['course.id'], r['course.name'], r['credit'],
                        r['class_hour'], CourseGrading[r['grading']])
                        for r in res]
            else:
                raise EntityNotFoundError

    async def enroll_course(self, student_id: int, section_id: int) \
            -> EnrollResult:
        async with self.__pool.acquire() as con:
            con.close()

# @throws Exception if the student already has a grade for the course section.
    async def drop_course(self, student_id: int, section_id: int):
        async with self.__pool.acquire() as con:
            con.close()

    async def add_enrolled_course_with_grade(self, student_id: int,
                                             section_id: int,
                                             grade: Optional[Grade]):
        raise NotImplementedError

    async def set_enrolled_course_grade(self, student_id: int,
                                        section_id: int, grade: Grade):
        raise NotImplementedError

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
