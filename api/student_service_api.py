import asyncpg
import datetime
from service.student_service import StudentService
from typing import List, Mapping, Optional

from dto import (Course, CourseSearchEntry, CourseTable, CourseType,
                 DayOfWeek, EnrollResult, Grade, Major)


class student_service(StudentService):

    def __init__ (self, pool: asyncpg.Pool):
        self.__pool = pool;
        

    async def add_student(self, user_id: int, major_id: int, first_name: str,
                          last_name: str, enrolled_date: datetime.date):
        async with self.__pool.acquire() as conn:
            await conn.excute('''
            insert into student (id, full_name, enrolled_date, major)
            values (%d, %s, %s, %d)
            ''' % (user_id, last_name+' '+first_name, enrolled_date, major_id))

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
        raise NotImplementedError

    async def enroll_course(self, student_id: int, section_id: int) \
            -> EnrollResult:
        raise NotImplementedError

    async def drop_course(self, student_id: int, section_id: int):
        raise NotImplementedError

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
        raise NotImplementedError
