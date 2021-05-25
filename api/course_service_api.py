import asyncpg
from service.course_service import CourseService
from typing import List, Optional

from dto import (Course, CourseGrading, CourseSection, CourseSectionClass,
                 DayOfWeek, Prerequisite, Student)


class course_service(CourseService):

    def __init__ (self, pool: asyncpg.Pool):
        self.__pool = pool;

    #TODO: prerequisite
    async def add_course(self, course_id: str, course_name: str, credit: int,
                         class_hour: int, grading: CourseGrading,
                         prerequisite: Optional[Prerequisite]):
        async with self.__pool.acquire() as conn:
            await conn.execute('''
                insert into course (id, name, credit, class_hour, grading)
                values (%s, %s, %d, %d, %s)
            ''' % (course_id, course_name, credit, class_hour, grading.name))
            #await conn.execute('''insert into prerequisite ())

    async def add_course_section(self, course_id: str, semester_id: int,
                                 section_name: str, total_capacity: int
                                 ) -> int:
        async with self.__pool.acquire() as conn:
            await conn.fetchval()

    async def add_course_section_class(self, section_id: int,
                                       instructor_id: int,
                                       day_of_week: DayOfWeek,
                                       week_list: List[int],
                                       class_start: int,
                                       class_end: int,
                                       location: str) -> int:
        raise NotImplementedError

    async def remove_course(self, course_id: str):
        raise NotImplementedError

    async def remove_course_section(self, section_id: int):
        raise NotImplementedError

    async def remove_course_section_class(self, class_id: int):
        raise NotImplementedError

    async def get_all_courses(self) -> List[Course]:
        raise NotImplementedError

    async def get_course_sections_in_semester(self, course_id: str,
                                              semester_id: int
                                              ) -> List[CourseSection]:
        raise NotImplementedError

    async def get_course_by_section(self, section_id: int) -> Course:
        raise NotImplementedError

    async def get_course_section_classes(self, section_id: int) \
            -> List[CourseSectionClass]:
        raise NotImplementedError

    async def get_course_section_by_class(self, class_id: int) \
            -> CourseSection:
        raise NotImplementedError

    async def get_enrolled_students_in_semester(self, course_id: str,
                                                semester_id: int
                                                ) -> List[Student]:
        raise NotImplementedError
