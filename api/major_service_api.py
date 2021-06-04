from exception import EntityNotFoundError, IntegrityViolationError
import asyncpg
from service.major_service import MajorService
from typing import List

from dto import Major, Department


class major_service(MajorService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_major(self, name: str, department_id: int) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into major (name, department) values ('%s', %d)
                    returning id
                ''' % (name, department_id))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def remove_major(self, major_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from major where id='+major_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def get_all_majors(self) -> List[Major]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select major.id, major.name as major_name, department,
                department.name as department_name
            from major
                join department on major.department = department.id
            ''')
            if res:
                return [Major(r['id'],
                              r['major_name'],
                              Department(r['department'],
                                         r['department_name']))
                        for r in res]
            else:
                return []

    async def get_major(self, major_id: int) -> Major:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('''
            select major.id, major.name as major_name, department,
                department.name as department_name
            from major
                join department on major.department = department.id
            where major.id = %d
            ''' % major_id)
            if res:
                return Major(res['id'],
                             res['major_name'],
                             Department(res['department'],
                                        res['department_name']))
            else:
                raise EntityNotFoundError

    async def add_major_compulsory_course(self, major_id: int, course_id: str):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into major_course (major_id, course_id, course_type)
                values (%d, '%s', '%s')
                ''' % (major_id, course_id, 'C'))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def add_major_elective_course(self, major_id: int, course_id: str):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into major_course (major_id, course_id, course_type)
                values (%d, '%s', '%s')
                ''' % (major_id, course_id, 'E'))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e
