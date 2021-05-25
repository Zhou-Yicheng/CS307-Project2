from service.department_service import DepartmentService
from typing import List

from dto import Department

class department_service(DepartmentService):

    def __init__(self, pool):
        self.__pool = pool;


    async def add_department(self, name: str) -> int:
        raise NotImplementedError

    async def remove_department(self, department_id: int):
        raise NotImplementedError

    async def get_all_departments(self) -> List[Department]:
        raise NotImplementedError

    async def get_department(self, department_id: int) -> Department:
        raise NotImplementedError
