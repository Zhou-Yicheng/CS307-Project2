from exception import EntityNotFoundError, IntegrityViolationError
import asyncpg
from service.major_service import MajorService
from typing import List

from dto import Major


class major_service(MajorService):

    def __inti__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_major(self, name: str, department_id: int) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into major (name, department) values (%s, %d) returning id
                ''' % (name, department_id))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def remove_major(self, major_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from major where id='+major_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    # TODO
    async def get_all_majors(self) -> List[Major]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('select * from major')
            return [Major(r['id'], r['name'], r['department']) for r in res]

    # TODO
    async def get_major(self, major_id: int) -> Major:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('select * from major where id='+major_id)
            if res:
                return Major(res['id'], res['name'], res['department'])
            else:
                raise EntityNotFoundError

    async def add_major_compulsory_course(self, major_id: int, course_id: str):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into major_course (major_id, course_id, course_type) values (%d, %s, %s)
                ''' % (major_id, course_id, 'C'))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def add_major_elective_course(self, major_id: int, course_id: str):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into major_course (major_id, course_id, course_type) values (%d, %s, %s)
                ''' % (major_id, course_id, 'E'))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e
