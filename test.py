# import asyncio
# import asyncpg


# async def main():
#     pool = await asyncpg.create_pool(host='localhost',
#                                      port='5432',
#                                      database='project2',
#                                      user='postgres',
#                                      password='postgres')
#     for i in range(100):
#         print(i, end=' ')
#         print(await pool.fetchval("select cast('%d' as integer) between 0 and 100" % i))

# asyncio.get_event_loop().run_until_complete(main())
# #     async with pool.acquire() as conn:
# #         # r = await conn.execute("delete from student where full_name = 'zyc'")
# #         res = await conn.fetch("select * from student")
# #         print(res)
# #         print(type(res))
# #         if (res):
# #             print(res[0])
# #         else:
# #             print("Exception")

# # asyncio.get_event_loop().run_until_complete(main())

i = 1
print(type(i))
print('%s' % i)
