import asyncio
from dto import Student, Major
import asyncpg

async def main():
    pool = await asyncpg.create_pool(host='localhost',
                                port='5432',
                                database='project2',
                                user='postgres',
                                password='postgres')
    
    async with pool.acquire() as conn:
        # r = await conn.execute("delete from student where full_name = 'zyc'")
        res = await conn.fetch("select * from student")
        print(res)
        print(type(res))
        if (res):
            print(res[0])
        else:
            print("Exception")

asyncio.get_event_loop().run_until_complete(main())