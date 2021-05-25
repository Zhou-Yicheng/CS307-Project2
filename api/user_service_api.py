import asyncpg
from service.user_service import UserService
from typing import List

from dto import User


class user_service(UserService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool;


    async def remove_user(self, user_id: int):
        raise NotImplementedError

    async def get_all_users(self) -> List[User]:
        raise NotImplementedError

    async def get_user(self, user_id: int) -> User:
        raise NotImplementedError
