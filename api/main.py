from fastapi import FastAPI
from fastapi.responses import JSONResponse
from connection import Connection

app = FastAPI()

@app.get("/") #route - target location of request in which slash is the home
def get_data():
    try:
        connection = Connection()
        connection.db_connect()

        if not connection.db_connected():
            return JSONResponse(content={"error: Database not connected"})

        with connection.db.cursor() as cursor:
            query =f"""
                SELECT * FROM train
            
            """
            cursor.execute(query)
            result = cursor.fetchall()

        #return JSONResponse(content={"datas": result})
        return result
    except Exception as e:
        print(f"Error: {str(e)}")