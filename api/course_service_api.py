from typing import List, Optional

import asyncpg
from dto import (AndPrerequisite, Course, CourseGrading, CoursePrerequisite,
                 CourseSection, CourseSectionClass, DayOfWeek, Department,
                 Instructor, Major, OrPrerequisite, Prerequisite, Student)
from exception import EntityNotFoundError, IntegrityViolationError
from service.course_service import CourseService


class course_service(CourseService):

    def __init__(self, pool: asyncpg.Pool):
        self.__pool = pool

    async def add_course(self, course_id: str, course_name: str, credit: int,
                         class_hour: int, grading: CourseGrading,
                         prerequisite: Optional[Prerequisite]):
        async with self.__pool.acquire() as con:
            try:
                await con.execute('''
                insert into course (id, name, credit, class_hour, grading)
                values ('%s', '%s', %d, %d, '%s')
                ''' % (course_id, course_name, credit, class_hour, grading.name
                       ))
                if prerequisite:
                    stack = [0, prerequisite]
                    while stack:
                        i = stack.pop()
                        p = stack.pop()
                        if isinstance(p, AndPrerequisite):
                            v = 'AND'
                        elif isinstance(p, OrPrerequisite):
                            v = 'OR'
                        elif isinstance(p, CoursePrerequisite):
                            v = p.course_id
                        else:
                            pass

                        j = i
                        ptr = []
                        for c in p.terms:
                            j += 1
                            ptr.append(j)
                            stack.append(j)
                            stack.append(c)

                        await con.execute('''
                        insert into prerequisite (id, idx, val, ptr)
                        values (%d, %d, '%s', array%s)
                        ''' % (course_id, i, v, ptr))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def add_course_section(self, course_id: str, semester_id: int,
                                 section_name: str, total_capacity: int
                                 ) -> int:
        async with self.__pool.acquire() as con:
            try:
                return await con.fetchval('''
                insert into section (course, semester, name, total_capacity, \
                    left_capacity)
                values ('%s', %d, '%s', %d, %d)
                    returning id
                ''' % (course_id, semester_id, section_name, total_capacity,
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
                insert into class (section, instructor, day_of_week, \
                    week_list, class_begin, class_end)
                values (%d, %d, '%s', array%s, %d, %d, '%s')
                    returning id
                ''' % (section_id, instructor_id, day_of_week.name,
                       week_list, class_begin, class_end, location))
            except asyncpg.exceptions.IntegrityConstraintViolationError as e:
                raise IntegrityViolationError from e

    async def remove_course(self, course_id: str):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from course where id='+course_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def remove_course_section(self, section_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from section where id='+section_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def remove_course_section_class(self, class_id: int):
        async with self.__pool.acquire() as con:
            res = await con.execute('delete from class where id='+class_id)
            if res == 'DELETE 0':
                raise EntityNotFoundError

    async def get_all_courses(self) -> List[Course]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('select * from course')
            if res:
                return [Course(r['id'],
                               r['name'],
                               r['credit'],
                               r['class_hour'],
                               CourseGrading[r['grading']]
                               ) for r in res]
            else:
                return []

    async def get_course_sections_in_semester(self, course_id: str,
                                              semester_id: int
                                              ) -> List[CourseSection]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from section
            where course = '%s' and semester = %d
            ''' % (course_id, semester_id))
            if res:
                return [CourseSection(r['id'],
                                      r['name'],
                                      r['total_capacity'],
                                      r['left_capacity']
                                      ) for r in res]
            else:
                raise EntityNotFoundError

    async def get_course_by_section(self, section_id: int) -> Course:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('''
            select *
            from course
            where id = (select course from section where section.id = %d)
            ''' % section_id)
            if res:
                return Course(res['id'],
                              res['name'],
                              res['credit'],
                              res['class_hour'],
                              CourseGrading[res['grading']]
                              )
            else:
                raise EntityNotFoundError

    async def get_course_section_by_class(self, class_id: int) \
            -> CourseSection:
        async with self.__pool.acquire() as con:
            res = await con.fetchrow('''
            select *
            from section
            where id = (select section from class where class.id = %d)
            ''' % class_id)
            if res:
                return CourseSection(res['id'],
                                     res['name'],
                                     res['total_capacity'],
                                     res['left_capacity']
                                     )
            else:
                raise EntityNotFoundError

    async def get_course_section_classes(self, section_id: int) \
            -> List[CourseSectionClass]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select *
            from class
                join instructor on class.instructor = instructor.id
            where section = %d
            ''' % section_id)
            if res:
                return [CourseSectionClass(r['class.id'],
                                           Instructor(r['instructor'],
                                                      r['full_name']),
                                           DayOfWeek[r['day_of_week']],
                                           r['week_list'],
                                           r['class_begin'],
                                           r['class_end'],
                                           r['location']
                                           ) for r in res]
            else:
                raise EntityNotFoundError

    async def get_enrolled_students_in_semester(self, course_id: str,
                                                semester_id: int
                                                ) -> List[Student]:
        async with self.__pool.acquire() as con:
            res = await con.fetch('''
            select student.*, major.*, department.*
            from student
                join takes on student.id = student_id
                join section on section_id = section.id
                join major on major = major.id
                join department on department = department.id
            where course = %d and semester = %d
            ''' % (course_id, semester_id))
            if res:
                return [Student(r['student.id'],
                                r['full_name'],
                                r['enrolled_date'],
                                Major(r['major.id'],
                                      r['major.name'],
                                      Department(r['department.id'],
                                                 r['department.name'])
                                      )
                                ) for r in res]
            else:
                raise EntityNotFoundError

        # async with self.__pool.acquire() as con:
        #     res = await con.fetch('''
        #     select * from
        #     select *
        #     from student
        #     where id in (
        #         select student_id
        #         from takes
        #         where section_id in (
        #             select section.id
        #             from section
        #             where course = %d and semester = %d
        #         )
        #     ) s
        #     join major on s.major = major.id
        #     join department on s.department = department.id
        #     ''' % (course_id, semester_id))
        #     if res:
        #         return [Student(r['student.id'],
        #                         r['full_name'],
        #                         r['enrolled_date'],
        #                         Major(r['major'],
        #                               r['major.name'],
        #                               Department(r['department'],
        #                                          r['department.name'])
        #                               )
        #                         ) for r in res]
        #     else:
        #         raise EntityNotFoundError
