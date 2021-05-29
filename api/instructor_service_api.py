from collections import UserDict
from exception import IntegrityViolationError
import asyncpg
from service.instructor_service import InstructorService
from typing import List

from dto import CourseSection


class instructor_service(InstructorService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_instructor(self, user_id: int, first_name: str,
                             last_name: str):
        if str.isalpha(first_name) and str.isalpha(last_name):
            full_name = first_name + ' ' + last_name
        else:
            full_name = last_name+first_name
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into instructor (id, full_name) values (%d, %s)
                ''' % (user_id, full_name))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e


    async def get_instructed_course_sections(self, instructor_id: int,
                                             semester_id: int
                                             ) -> List[CourseSection]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select section.id, section.name, section.total_capacity, section.left_capacity
            from instructor join class on instructor.id = class.instructor
                join section on class.section = section.id
            where instructor.id = %d and semester = %d
            ''' % (instructor_id, semester_id))
            return [CourseSection(r['id'], r['name'],
            r['total_capacity'], r['left_capacity']) for r in res]
