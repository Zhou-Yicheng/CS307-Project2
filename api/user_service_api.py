from exception import EntityNotFoundError
import asyncpg
from service.user_service import UserService
from typing import List

from dto import Instructor, Student, User


class user_service(UserService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def remove_user(self, user_id: int):
        async with self.__pool.acquire() as con:
            res1 = await con.execute('delete from instructor where id='+user_id)
            res2 = await con.execute('delete from student where id='+user_id)
            if res1 == 'DELETE 0' and res2 == 'DELETE 0':
                raise EntityNotFoundError

    async def get_all_users(self) -> List[User]:
        async with self.__pool.acquire() as con:
            instructors = await con.fetch('select * from instructor')
            students = await con.fetch('select * from student')
            if(instructors and students):
                return ([Instructor(i['id'], i['full_name']) for i in instructors]
                        + [Student(s['id'], s['full_name']) for s in students])
            elif(instructors):
                return [Instructor(i['id'], i['full_name']) for i in instructors]
            elif(students):
                return [Student(s['id'], s['full_name']) for s in students]
            else:
                raise EntityNotFoundError

    async def get_user(self, user_id: int) -> User:
        async with self.__pool.acquire() as con:
            s = await con.fetchrow('select * from student where id='+user_id)
            i = await con.fetchrow('select * from instructor where id='+user_id)
            if s:
                return Student(s['id'], s['full_name'])
            elif i:
                return Instructor(i['id'], i['full_name'])
            else:
                return EntityNotFoundError
