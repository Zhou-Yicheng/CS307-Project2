from exception import EntityNotFoundError, IntegrityViolationError
import asyncpg
from service.semester_service import SemesterService
from datetime import date
from typing import List

from dto import Semester


class semester_service(SemesterService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_semester(self, name: str, begin: date, end: date) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into semester (name,begin_date,end_date) values (%(name)s, %(begin)s, %(end)s) returning id
                ''' % {'name': name, 'begin': begin, 'end': end})
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def remove_semester(self, semester_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from department where id ='+semester_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def get_all_semesters(self) -> List[Semester]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('select * from semester')
            return [Semester(r['id'], r['name'], r['begin_date'], r['end_date']) for r in res]

    async def get_semester(self, semester_id: int) -> Semester:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('select id, name, begin, end from department where id ='+semester_id)
            if res:
                return Semester(res['id'], res['name'], res['begin'], res['end'])
            else:
                return EntityNotFoundError
