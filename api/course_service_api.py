import asyncpg
from service.course_service import CourseService
from typing import List, Optional
from exception import EntityNotFoundError, IntegrityViolationError
from dto import (Course, CourseGrading, CourseSection, CourseSectionClass,
                 DayOfWeek, Prerequisite, Student)


class course_service(CourseService):

    def __init__ (self, pool: asyncpg.Pool):
        self.__pool = pool;


    #TODO: prerequisite
    async def add_course(self, course_id: str, course_name: str, credit: int,
                         class_hour: int, grading: CourseGrading,
                         prerequisite: Optional[Prerequisite]):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into course (id, name, credit, class_hour, grading)
                values (%s, %s, %d, %d, %s)
                ''' % (course_id, course_name, credit, class_hour, grading.name))
                if (prerequisite):
                    pass


            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e


    async def add_course_section(self, course_id: str, semester_id: int,
                                 section_name: str, total_capacity: int
                                 ) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into section (course, semester, name, total_capacity,\
                    left_capacity)
                values (%s, %d, %s, %d, %d) returning id
                ''' % (course_id, semester_id, section_name, total_capacity,\
                        total_capacity))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e


    async def add_course_section_class(self, section_id: int,
                                       instructor_id: int,
                                       day_of_week: DayOfWeek,
                                       week_list: List[int],
                                       class_begin: int,
                                       class_end: int,
                                       location: str) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into class (section, instructor, day_of_week,\
                    week_list, class_begin, class_end)
                values (%d, %d, %d, %s, %d, %d, %s) returning id
                ''' % (section_id, instructor_id, day_of_week,\
                        week_list, class_begin, class_end, location))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e


    async def remove_course(self, course_id: str):
        async with self.__pool.acquire() as con:
            exe = await con.execute('delete from course where id='+course_id)
            if (exe == 'DELETE 0'):
                raise EntityNotFoundError


    async def remove_course_section(self, section_id: int):
        async with self.__pool.acquire() as con:
            exe = await con.execute('delete from section where id='+section_id)
            if (exe == 'DELETE 0'):
                raise EntityNotFoundError


    async def remove_course_section_class(self, class_id: int):
        async with self.__pool.acquire() as con:
            exe = await con.execute('delete from class where id =' + class_id)
            if (exe == 'DELETE 0'):
                raise EntityNotFoundError


    #TODO: return a list of Course
    async def get_all_courses(self) -> List[Course]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('select * from course')


    #TODO: return a list of Section
    #TODO: compbinded index on section of course and semester
    async def get_course_sections_in_semester(self, course_id: str,
                                              semester_id: int
                                              ) -> List[CourseSection]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from section
            where course = %s and semester = %d
            ''' % (course_id, semester_id))
            if (res):
                pass
            else:
                raise EntityNotFoundError


    async def get_course_by_section(self, section_id: int) -> Course:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('select course from section where id=' \
                + section_id)
            if (res):
                return Course(res['id'], res['name'], res['credit'],\
                    res['class_hour'], res['grading'])
            else:
                raise EntityNotFoundError


    #TODO: return a list of Class
    #TODO: index on class of section
    async def get_course_section_classes(self, section_id: int) \
            -> List[CourseSectionClass]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from class
            where section = %d
            ''' % section_id)
            if (res):
                pass
            else:
                raise EntityNotFoundError


    async def get_course_section_by_class(self, class_id: int) \
            -> CourseSection:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('select section from class where id=' \
                + class_id)
            if (res):
                return CourseSection(res['id'], res['name'],\
                    res['total_capacity'], res['left_capacity'])
            else:
                raise EntityNotFoundError


    async def get_enrolled_students_in_semester(self, course_id: str,
                                                semester_id: int
                                                ) -> List[Student]:
        async with self.__pool.acquire() as con:
            return await con.fetch('''
            select student.id, full_name, enrolled_date, major
            from takes join section on section_id = section.id
                join student on student_id = student.id
            where section.course = %s and semester = %d
            ''' % (course_id, semester_id))
