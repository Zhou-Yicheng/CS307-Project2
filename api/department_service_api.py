import asyncpg
from exception import EntityNotFoundError, IntegrityViolationError
from service.department_service import DepartmentService
from typing import List
from dto import Department

class department_service(DepartmentService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_department(self, name: str) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into department (name) values (%s) returning id
                ''' % (name))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def remove_department(self, department_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from department where id='+department_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def get_all_departments(self) -> List[Department]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('select * from department')
            return [Department(r['id'], r['name']) for r in res]

    async def get_department(self, department_id: int) -> Department:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('select * from department where id ='+department_id)
            if res:
                return Department(res['id'], res['name'])
            else:
                return EntityNotFoundError
