import asyncpg
from service.instructor_service import InstructorService
from typing import List

from dto import CourseSection


class instructor_service(InstructorService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool


    async def add_instructor(self, user_id: int, first_name: str,
                             last_name: str):
        raise NotImplementedError

    async def get_instructed_course_sections(self, instructor_id: int,
                                             semester_id: int
                                             ) -> List[CourseSection]:
        raise NotImplementedError
