from configparser import ConfigParser
from pathlib import Path

import asyncpg

from service import (CourseService, DepartmentService, InstructorService,
                     MajorService, SemesterService, StudentService,
                     UserService)
from api import (course_service, department_service, instructor_service,
                 major_service, semester_service, student_service,
                 user_service)


def create_async_context():
    # You can customize the async context manager in this function.
    # e.g., you may use other connection pool implementation.
    config = ConfigParser()
    config.read(Path(__file__).parent / 'config.ini')
    db_cfg = config['database']
    return asyncpg.create_pool(host=db_cfg['host'],
                               port=db_cfg['port'],
                               database=db_cfg['database'],
                               user=db_cfg['username'],
                               password=db_cfg['password'])


class ServiceFactory:
    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool
        self.__pool._minsize = 10
        self.__pool._maxsize = 20

    async def async_init(self):
        # You can add asynchronous initialization steps here.
        # return await self.__pool._async__init__()
        if self.__pool._initialized:
            return
        if self.__pool._initializing:
            raise asyncpg.exceptions.InterfaceError
        if self.__pool._closed:
            raise asyncpg.exceptions.InterfaceError
        self.__pool._initializing = True
        try:
            await self.__pool._initialize()
        finally:
            self.__pool._initializing = False
            self.__pool._initialized = True

    def create_course_service(self) -> CourseService:
        return course_service(self.__pool)

    def create_department_service(self) -> DepartmentService:
        return department_service(self.__pool)

    def create_instructor_service(self) -> InstructorService:
        return instructor_service(self.__pool)

    def create_major_service(self) -> MajorService:
        return major_service(self.__pool)

    def create_semester_service(self) -> SemesterService:
        return semester_service(self.__pool)

    def create_student_service(self) -> StudentService:
        return student_service(self.__pool)

    def create_user_service(self) -> UserService:
        return user_service(self.__pool)
